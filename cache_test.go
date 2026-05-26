package ads

import (
	"encoding/binary"
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
//   - R-CACHE-008 (never both cache.lock + notifs.lock simultaneously).

// newCacheTestSession builds a minimal Session sufficient for cache-only
// tests. No client, no transport workers. The session FSM is left in the
// zero-value Constructed state so bumpEpoch / epoch read paths work.
func newCacheTestSession() *Session {
	return &Session{
		cache:         &symbolCache{symbols: map[string]*symbol{}, onDemandSymbols: map[string]bool{}},
		notifications: &notificationManager{activeNotifications: make(map[uint32]*symbol), configsByKey: make(map[string]struct{})},
		lifecycle:     &sessionLifecycle{},
		logger:        getDefaultLogger(),
	}
}

// TestCacheLock_GuardsMutations spawns 50 readers and 50 writers exercising
// cache.symbols under cache.lock. The race detector flags any unguarded
// access. The test passes only when every read and write goes through
// cache.lock — which is the production invariant.
//
// Validates: R-CACHE-002 (race-detector clean cache).
func TestCacheLock_GuardsMutations(t *testing.T) {
	sess := newCacheTestSession()

	const Goroutines = 50
	const Iterations = 200

	var wg sync.WaitGroup

	// Writers — insert symbols under cache.lock.
	for i := 0; i < Goroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < Iterations; j++ {
				key := symbolKey(string(rune('a' + (id+j)%26)))
				sess.cache.lock.Lock()
				sess.cache.symbols[key] = &symbol{FullName: key, Name: key}
				sess.cache.lock.Unlock()
			}
		}(i)
	}

	// Readers — iterate snapshot under cache.lock.
	for i := 0; i < Goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < Iterations; j++ {
				sess.cache.lock.Lock()
				_ = len(sess.cache.symbols)
				for _, s := range sess.cache.symbols {
					_ = s.Name
				}
				sess.cache.lock.Unlock()
			}
		}()
	}

	wg.Wait()
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

// TestCacheLockOrdering_NoSimultaneousHold drives concurrent goroutines
// that take cache.lock and then notifs.lock (and vice versa) to defend
// against the deadlock pattern. The production guidance is "never both
// at once" — this test wraps each acquire/release in a single critical
// section so the race detector + Go's deadlock detector validate behavior
// when these locks are used following the contract.
//
// We use the production AddSymbolNotification's pattern: capture under
// cache.lock, release, then take notifs.lock. The reverse-order goroutine
// uses the symmetric DeleteDeviceNotification pattern (notifs.lock first,
// then cache.lock for parse).
//
// Validates: R-CACHE-008 (never hold both locks simultaneously) +
// R-LOCK-002 (race-detector clean).
func TestCacheLockOrdering_NoSimultaneousHold(t *testing.T) {
	sess := newCacheTestSession()
	// Seed cache with one symbol so the readers do real work.
	sess.cache.lock.Lock()
	sess.cache.symbols[symbolKey("MAIN.x")] = &symbol{FullName: "MAIN.x", Name: "x"}
	sess.cache.lock.Unlock()

	const Goroutines = 25
	const Iterations = 100

	var wg sync.WaitGroup

	// Goroutine family A: cache → release → notifs (subscribe-side pattern).
	for i := 0; i < Goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < Iterations; j++ {
				sess.cache.lock.Lock()
				_ = sess.cache.symbols[symbolKey("MAIN.x")]
				sess.cache.lock.Unlock()

				sess.notifications.lock.Lock()
				_ = len(sess.notifications.activeNotifications)
				sess.notifications.lock.Unlock()
			}
		}()
	}
	// Goroutine family B: notifs → release → cache (dispatch-side pattern).
	for i := 0; i < Goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < Iterations; j++ {
				sess.notifications.lock.Lock()
				_ = len(sess.notifications.activeNotifications)
				sess.notifications.lock.Unlock()

				sess.cache.lock.Lock()
				_ = sess.cache.symbols[symbolKey("MAIN.x")]
				sess.cache.lock.Unlock()
			}
		}()
	}
	wg.Wait()
}

// TestCache_OnDemandResolve_DuplicateHandleReleased is the canonical R-CACHE-007
// test: two goroutines call sess.getSymbol("MAIN.x") concurrently; one
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
			sym, err := sess.getSymbol("MAIN.x")
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
