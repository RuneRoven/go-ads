package ads

import (
	"context"
	"encoding/binary"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// cache_test.go — symbolCache unit tests.
//
// Covers:
//   - R-CACHE-002 (cache.lock guards mutations under -race)
//   - R-CACHE-003 (epoch bumps on swap, NOT on insert)
//   - R-CACHE-007 (concurrent on-demand resolve duplicate-handle release —
//     marked TODO until a stub Client can be wired without real PLC I/O).
//   - R-CACHE-008 — deliberately NOT covered by a runtime test; see the note
//     where its test used to live, below TestCacheEpoch_BumpsOnSwapNotInsert.

// newCacheTestSession builds a minimal Session sufficient for cache-only
// tests. No client, no transport workers. The session FSM is left in the
// zero-value Constructed state so bumpEpoch / epoch read paths work.
func newCacheTestSession() *Session {
	return &Session{
		cache:         &symbolCache{symbols: map[string]*symbol{}, onDemandSymbols: map[string]bool{}},
		notifications: &notificationManager{activeNotifications: make(map[uint32]activeNotification), configsByKey: make(map[string]struct{}), orphanSeen: make(map[uint32]time.Time), orphanSem: make(chan struct{}, orphanDeleteMaxConcurrency)},
		lifecycle:     &sessionLifecycle{},
		logger:        getDefaultLogger(),
	}
}

// TestCacheLock_GuardsMutations drives the two PRODUCTION families that touch
// cache.symbols concurrently: the on-demand resolve path (sess.getSymbol, which
// inserts into cache.symbols) and the notification dispatch path (drivePacket →
// dispatchSample, which reads the map and mutates the symbol under parse). The
// race detector is the oracle for the locking; the assertions below pin that both
// families actually did their work, so a silently-empty run cannot pass.
//
// The version this replaced hand-rolled cache.lock Lock/Unlock in test code and
// called no production function at all — stripping every production cache.lock
// call from the package left it green under -race. This shape goes red under
// that same mutation (verified before landing). Note two other tests already
// kill it — TestAddSymbolNotification_StrandedSymbol_DetectedByEpoch and
// TestReadFromSymbol_SymbolNotFoundReResolvesTheCachedHandle — so this is the
// dedicated pin, not the only one; do not delete those thinking it is.
//
// Validates: R-CACHE-002 (race-detector clean cache).
func TestCacheLock_GuardsMutations(t *testing.T) {
	srv := startScriptableServer(t)
	defer srv.stop()

	// Name-aware symbol info so every distinct name resolves to its own symbol.
	srv.onWriteRead(GroupSymbolInfoByNameEx, func(req []byte) []byte {
		name := strings.TrimRight(string(req), "\x00")
		return buildSymbolInfoPayload(name, "INT", "", 0x4040, 0x100, 2, ADSTInt16, 0)
	})
	var nextHandle atomic.Uint32
	srv.onWriteRead(GroupSymbolHandleByName, func(_ []byte) []byte {
		return buildHandlePayload(nextHandle.Add(1))
	})
	srv.onWrite(GroupSymbolReleaseHandle, func(_, _ uint32, _ []byte) ReturnCode {
		return ReturnCodeNoErrors
	})

	sess, client := newWiredTestSession(t, srv)
	// The helper leaves the notification callback unset; without it drivePacket
	// decodes the packet and dispatches into nothing.
	client.SetNotificationHandler(sess.handleNotification)
	ctx := context.Background()

	// The dispatch side needs one live subscription. Resolve its symbol through
	// production getSymbol, then register the handle: seeding activeNotifications
	// is setup, the cache access under test is dispatchSample's own.
	const dispatchName = "MAIN.dispatched"
	const dispatchHandle uint32 = 0x0BAD0001
	dispatched, err := sess.getSymbol(ctx, dispatchName)
	if err != nil {
		t.Fatalf("resolving %s: %v", dispatchName, err)
	}
	updates := make(chan *Update, 1)
	sess.notifications.lock.Lock()
	sess.notifications.activeNotifications[dispatchHandle] = activeNotification{Sym: dispatched, Ch: updates}
	sess.notifications.lock.Unlock()

	sample := make([]byte, 2)
	binary.LittleEndian.PutUint16(sample, 42)
	packet := buildNotificationPacket(dispatchHandle, 0, sample)

	const (
		resolvers   = 8
		dispatchers = 8
		resolves    = 25
		dispatches  = 200
	)

	resolveErrs := make([]error, resolvers*resolves)
	var wg sync.WaitGroup

	// Writers — production on-demand resolve inserts a fresh cache entry.
	for i := 0; i < resolvers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < resolves; j++ {
				name := fmt.Sprintf("MAIN.w%d_%d", id, j)
				sym, err := sess.getSymbol(ctx, name)
				switch {
				case err != nil:
					resolveErrs[id*resolves+j] = err
				case sym == nil || sym.Handle == 0:
					resolveErrs[id*resolves+j] = fmt.Errorf("%s resolved to %+v, want a symbol with a handle", name, sym)
				}
			}
		}(i)
	}

	// Readers — production dispatch reads cache.symbols and parses under cache.lock.
	for i := 0; i < dispatchers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < dispatches; j++ {
				if err := sess.drivePacket(ctx, packet); err != nil {
					t.Errorf("drivePacket: %v", err)
					return
				}
			}
		}()
	}

	// A lock-order inversion between these two families deadlocks rather than
	// races, and a deadlock must report as a failure, not as a package timeout.
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(60 * time.Second):
		t.Fatal("resolve and dispatch did not finish in 60s: cache.lock/notifications.lock deadlock?")
	}

	for _, err := range resolveErrs {
		if err != nil {
			t.Fatalf("on-demand resolve failed: %v", err)
		}
	}
	sess.cache.lock.Lock()
	cached := len(sess.cache.symbols)
	value, valid := dispatched.Value, dispatched.Valid
	sess.cache.lock.Unlock()
	if want := resolvers*resolves + 1; cached != want {
		t.Errorf("cache.symbols size = %d, want %d", cached, want)
	}
	// Proof the dispatch family reached the parse section, not just the
	// unknown-handle early return.
	if value != "42" || !valid {
		t.Errorf("dispatched symbol Value = %q, Valid = %v; want \"42\", true — dispatch never parsed a sample", value, valid)
	}
}

// TestCacheEpoch_BumpsOnSwapNotInsert confirms cache.generation (now
// sessionFSM.epoch) is incremented when cache.symbols is REPLACED, not on
// in-place insert.
//
// Validates: R-CACHE-003 (generation increments on swap, NOT on insert).
func TestCacheEpoch_BumpsOnSwapNotInsert(t *testing.T) {
	sess := newCacheTestSession()

	// In-place insert (mimicking on-demand getSymbol): epoch unchanged.
	before := sess.epoch()
	sess.cache.lock.Lock()
	sess.cache.symbols[symbolKey("a")] = &symbol{FullName: "a"}
	sess.cache.lock.Unlock()
	if got := sess.epoch(); got != before {
		t.Errorf("after insert: epoch = %d, want unchanged %d", got, before)
	}

	// Swap (mimicking loadSymbols): bumpEpoch under cache.lock, epoch++.
	sess.cache.lock.Lock()
	sess.cache.symbols = map[string]*symbol{symbolKey("b"): {FullName: "b"}}
	sess.bumpEpoch()
	sess.cache.lock.Unlock()
	if got := sess.epoch(); got != before+1 {
		t.Errorf("after swap: epoch = %d, want %d", got, before+1)
	}

	// Second insert into the new map: still no bump.
	mid := sess.epoch()
	sess.cache.lock.Lock()
	sess.cache.symbols[symbolKey("c")] = &symbol{FullName: "c"}
	sess.cache.lock.Unlock()
	if got := sess.epoch(); got != mid {
		t.Errorf("after second insert: epoch = %d, want unchanged %d", got, mid)
	}
}

// R-CACHE-008 ("never hold cache.lock and notifications.lock simultaneously")
// has no runtime test here, deliberately. TestCacheLockOrdering_NoSimultaneousHold
// used to sit at this spot; it hand-rolled both lock families in test code and
// released lock 1 before taking lock 2, so the simultaneous hold it named could
// not occur in its own code and the production paths were never invoked — it
// survived stripping every production cache.lock call in the package.
//
// It was deleted rather than rewritten because the invariant is STATIC: no
// production path holds both locks (notification_api.go:19, and the ordering
// comments in dispatchSample). A deadlock only materialises when TWO paths each
// hold both in opposite order, so no single-site mutation can turn a runtime
// probe red — verifying a rewrite would mean injecting the bug twice, in two
// files, which is not a defect any realistic edit produces. What is worth
// keeping — a deadlock reporting as a failure instead of a package timeout — is
// carried by the bounded wg.Wait in TestCacheLock_GuardsMutations above, which
// drives the subscribe/resolve and dispatch families concurrently through
// production code.

// TestCache_OnDemandResolve_DuplicateHandleReleased is the canonical R-CACHE-007
// test: two goroutines call sess.getSymbol(context.Background(), "MAIN.x") concurrently; one
// must observe the in-cache symbol from the other; the loser must release
// the duplicate handle to the PLC via Write(GroupSymbolReleaseHandle, ...).
//
// Validates: R-CACHE-007 (concurrent on-demand resolve duplicate-handle release).
func TestCache_OnDemandResolve_DuplicateHandleReleased(t *testing.T) {
	srv := startScriptableServer(t)
	defer srv.stop()

	const fakeHandle uint32 = 0xCAFE0001
	srv.onWriteRead(GroupSymbolInfoByNameEx, func(_ []byte) []byte {
		return buildSymbolInfoPayload(
			"MAIN.x", "INT", "",
			0x4040, 0x100, 2, ADSTInt16, 0)
	})
	srv.onWriteRead(GroupSymbolHandleByName, func(_ []byte) []byte {
		return buildHandlePayload(fakeHandle)
	})
	// Inject a small delay before GetHandleByName so both goroutines pass
	// the cache.lock check, hit the network, and race on commit.
	srv.delayBefore(CommandIDReadWrite, uint32(GroupSymbolHandleByName), 50*time.Millisecond)

	var releases atomic.Int32
	srv.onWrite(GroupSymbolReleaseHandle, func(_, _ uint32, data []byte) ReturnCode {
		if len(data) == 4 && binary.LittleEndian.Uint32(data) == fakeHandle {
			releases.Add(1)
		}
		return ReturnCodeNoErrors
	})

	sess, _ := newWiredTestSession(t, srv)

	const N = 4
	results := make([]*symbol, N)
	errs := make([]error, N)
	var wg sync.WaitGroup
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			sym, err := sess.getSymbol(context.Background(), "MAIN.x")
			results[idx] = sym
			errs[idx] = err
		}(i)
	}
	wg.Wait()

	for i, e := range errs {
		if e != nil {
			t.Fatalf("getSymbol[%d]: %v", i, e)
		}
	}
	// All goroutines must observe the SAME *symbol pointer.
	for i := 1; i < N; i++ {
		if results[i] != results[0] {
			t.Errorf("results[%d] (%p) != results[0] (%p) — duplicate cache entries", i, results[i], results[0])
		}
	}
	// Cache must contain exactly one entry for MAIN.x.
	sess.cache.lock.Lock()
	got := len(sess.cache.symbols)
	sess.cache.lock.Unlock()
	if got != 1 {
		t.Errorf("cache.symbols size = %d, want 1", got)
	}
	// At least one duplicate handle must have been released. Production
	// code's TOCTOU loser releases its just-acquired handle. With N=4 and
	// a 50ms delay all four hit the network; one wins the commit race and
	// 3 must release. Allow >=1 to keep the assertion robust against
	// scheduler variations on slow CI.
	if rel := releases.Load(); rel < 1 {
		t.Errorf("ReleaseHandle calls = %d, want at least 1 (duplicate-handle release path)", rel)
	}
}
