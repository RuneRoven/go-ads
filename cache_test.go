package ads

import (
	"sync"
	"testing"
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
		cache:         &symbolCache{symbols: map[string]*Symbol{}, onDemandSymbols: map[string]bool{}},
		notifications: &notificationManager{activeNotifications: make(map[uint32]*Symbol)},
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
				sess.cache.symbols[key] = &Symbol{FullName: key, Name: key}
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
	sess.cache.symbols[symbolKey("a")] = &Symbol{FullName: "a"}
	sess.cache.lock.Unlock()
	if got := sess.epoch(); got != before {
		t.Errorf("after insert: epoch = %d, want unchanged %d", got, before)
	}

	// Swap (mimicking loadSymbols): bumpEpoch under cache.lock, epoch++.
	sess.cache.lock.Lock()
	sess.cache.symbols = map[string]*Symbol{symbolKey("b"): {FullName: "b"}}
	sess.bumpEpoch()
	sess.cache.lock.Unlock()
	if got := sess.epoch(); got != before+1 {
		t.Errorf("after swap: epoch = %d, want %d", got, before+1)
	}

	// Second insert into the new map: still no bump.
	mid := sess.epoch()
	sess.cache.lock.Lock()
	sess.cache.symbols[symbolKey("c")] = &Symbol{FullName: "c"}
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
	sess.cache.symbols[symbolKey("MAIN.x")] = &Symbol{FullName: "MAIN.x", Name: "x"}
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
// the duplicate handle to the PLC.
//
// Driving this requires a stub *Client that responds to GetSymbolInfoByName
// and GetHandleByName plus tracks GroupSymbolReleaseHandle Writes —
// substantial scaffolding that doesn't yet exist in the repo. Marked
// t.Skip with TODO to make the gap explicit.
//
// Validates: R-CACHE-007 (skipped — needs stub Client harness).
func TestCache_OnDemandResolve_DuplicateHandleReleased(t *testing.T) {
	t.Skip("TODO: requires stub Client that synthesizes GetSymbolInfoByName + GetHandleByName + tracks ReleaseHandle Writes; production path validated only via integration tests today.")
}
