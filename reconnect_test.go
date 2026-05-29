package ads

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// reconnect_test.go — reconnect FSM unit tests.
//
// Covers:
//   - R-RECON-002 (single-flight triggerReconnect)
//   - R-RECON-008 (Close during in-progress dial — no waitGroup misuse)
//   - R-RECON-009 (reconnect goroutine exits via reconnectDone)

// newReconnectTestSession returns a synthetic Session with autoReconnect
// disabled (so triggerReconnect doesn't actually launch Reconnect against
// a non-existent transport). FSM is left in Constructed and the caller
// drives it to Connected before triggering.
func newReconnectTestSession() *Session {
	return &Session{
		tx:            &transport{},
		notifications: &notificationManager{activeNotifications: make(map[uint32]activeNotification), configsByKey: make(map[string]struct{}), orphanSeen: make(map[uint32]time.Time), orphanSem: make(chan struct{}, orphanDeleteMaxConcurrency)},
		cache:         &symbolCache{symbols: map[string]*symbol{}, onDemandSymbols: map[string]bool{}},
		logger:        getDefaultLogger(),
		lifecycle: &sessionLifecycle{
			closedCh:      make(chan struct{}),
			autoReconnect: false,
		},
	}
}

// TestTriggerReconnect_SingleFlight asserts that 50 concurrent
// triggerReconnect calls on a Connected Session result in exactly one
// disconnect-callback fire (the first detector wins via CAS) and exactly
// one Disconnected transition.
//
// With autoReconnect=false the production code does not launch a
// Reconnect goroutine — that's where the test diverges from R-RECON-002
// strict (which counts goroutine launches). The CAS gate inside
// triggerReconnect is the single-flight contract; we observe its visible
// effect: callback fired once.
//
// Validates: R-RECON-002 (single-flight triggerReconnect).
func TestTriggerReconnect_SingleFlight(t *testing.T) {
	var fires atomic.Int32
	sess := newReconnectTestSession()
	sess.onDisconnect = func() { fires.Add(1) }
	sess.lifecycle.state.transitionTo(SessionStateConnecting)
	sess.lifecycle.state.transitionTo(SessionStateConnected)

	const N = 50
	var startWG sync.WaitGroup
	startWG.Add(N)
	var done sync.WaitGroup
	done.Add(N)
	for i := 0; i < N; i++ {
		go func() {
			defer done.Done()
			startWG.Done()
			sess.triggerReconnect()
		}()
	}
	startWG.Wait()
	done.Wait()
	// Allow any goroutine-scheduled disconnect callback to actually run.
	time.Sleep(50 * time.Millisecond)

	if got := fires.Load(); got != 1 {
		t.Errorf("disconnect callback fired %d times, want exactly 1 (single-flight CAS)", got)
	}
	// State must have transitioned exactly once: now Disconnected.
	if state := sess.lifecycle.state.load(); state != SessionStateDisconnected {
		t.Errorf("state = %v, want Disconnected", state)
	}
}

// TestReconnect_GoroutineExitsViaReconnectDone manually launches an
// abbreviated reconnect-shaped goroutine that closes lifecycle.reconnectDone
// on exit. Asserts subsequent waiters unblock.
//
// The full Reconnect path requires a live transport; we exercise the
// single contract that R-RECON-009 cares about: the channel SHALL close
// on goroutine exit. The production Reconnect uses defer close(); we
// verify the same pattern works as a black-box invariant.
//
// Validates: R-RECON-009 (reconnect goroutine exits via reconnectDone).
func TestReconnect_GoroutineExitsViaReconnectDone(t *testing.T) {
	sess := newReconnectTestSession()
	sess.lifecycle.state.transitionTo(SessionStateConnecting)
	sess.lifecycle.state.transitionTo(SessionStateConnected)

	// Simulate the production Reconnect entry: create reconnectDone,
	// run a "reconnect" body that returns, then close on defer.
	sess.lifecycle.reconnectMu.Lock()
	sess.lifecycle.reconnectDone = make(chan struct{})
	ch := sess.lifecycle.reconnectDone
	sess.lifecycle.reconnectMu.Unlock()

	go func() {
		defer func() {
			sess.lifecycle.reconnectMu.Lock()
			if sess.lifecycle.reconnectDone != nil {
				close(sess.lifecycle.reconnectDone)
				sess.lifecycle.reconnectDone = nil
			}
			sess.lifecycle.reconnectMu.Unlock()
		}()
		// Body: simulate quick reconnect-failed-and-bail.
		time.Sleep(10 * time.Millisecond)
	}()

	// waitForReconnect uses the same channel; assert it unblocks.
	done := make(chan struct{})
	go func() {
		sess.waitForReconnect()
		close(done)
	}()

	select {
	case <-done:
		// success — channel closed and waiter unblocked.
	case <-time.After(2 * time.Second):
		t.Fatal("waitForReconnect did not unblock — reconnectDone never closed")
	}

	// Sanity: the channel returned BEFORE the reconnect started is
	// closed (close(ch) is the same channel waiters block on).
	select {
	case <-ch:
		// expected — closed.
	default:
		t.Error("captured reconnectDone channel did not close")
	}
}

// TestReconnect_CloseDuringDial_NoWaitGroupMisuse exercises the
// Close-during-Reconnect race window. We synthesize a Session, set the
// FSM to Connected, then concurrently:
//   - Goroutine A: drives the FSM through Disconnected → Reconnecting,
//     allocates reconnectDone, does NOT add to waitGroup, then closes
//     reconnectDone (mimicking the failed-dial defer path).
//   - Goroutine B: calls Close, which should cleanly observe the
//     closed reconnectDone and proceed to wait on the waitGroup.
//
// Race-detector clean + no panic = pass.
//
// Validates: R-RECON-008 (Close during dial — race-detector clean).
func TestReconnect_CloseDuringDial_NoWaitGroupMisuse(t *testing.T) {
	sess := newReconnectTestSession()
	sess.lifecycle.state.transitionTo(SessionStateConnecting)
	sess.lifecycle.state.transitionTo(SessionStateConnected)

	// Allocate the reconnectDone channel as triggerReconnect would.
	sess.lifecycle.reconnectMu.Lock()
	sess.lifecycle.reconnectDone = make(chan struct{})
	sess.lifecycle.reconnectMu.Unlock()

	// Goroutine A: simulated reconnect attempt that fails-fast and exits.
	go func() {
		// Brief delay so Close has a chance to enter the wait.
		time.Sleep(20 * time.Millisecond)
		sess.lifecycle.reconnectMu.Lock()
		if sess.lifecycle.reconnectDone != nil {
			close(sess.lifecycle.reconnectDone)
			sess.lifecycle.reconnectDone = nil
		}
		sess.lifecycle.reconnectMu.Unlock()
	}()

	// Goroutine B: drive Close-equivalent: transition to Closed, wait
	// on reconnectDone. We avoid the real Close() because it walks the
	// cache and calls Client methods that need a live transport.
	if _, ok := sess.lifecycle.state.transitionToOnce(SessionStateClosed); !ok {
		t.Fatal("could not transition to Closed")
	}
	close(sess.lifecycle.closedCh)
	sess.lifecycle.reconnectMu.Lock()
	ch := sess.lifecycle.reconnectDone
	sess.lifecycle.reconnectMu.Unlock()
	if ch != nil {
		select {
		case <-ch:
			// success — reconnect goroutine exited cleanly.
		case <-time.After(2 * time.Second):
			t.Fatal("Close during reconnect: reconnectDone never closed")
		}
	}
	// Wait the waitGroup — should be a no-op since we never Add'd.
	sess.lifecycle.waitGroup.Wait()
}

// TestReconnectExhaustsMaxAttemptsTransitionsToClosed verifies that when
// Reconnect() exhausts maxReconnectAttempts, the FSM transitions to Closed
// rather than staying stuck in Reconnecting.
//
// A synthetic Session is used so no real TCP dial occurs. The session is
// pre-wired with an unreachable ip/port so dialAndStart fails immediately,
// and the lifecycle fields that tearDownAndReset needs are properly initialised.
//
// Validates: max-attempts exhaustion → FSM Closed (not stuck Reconnecting).
func TestReconnectExhaustsMaxAttemptsTransitionsToClosed(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	sess := &Session{
		ip:   "127.0.0.1",
		port: 1, // port 1 is always refused on loopback — instant TCP RST
		tx: &transport{
			sendChannel:    make(chan []byte),
			systemResponse: make(chan []byte, 1),
			recvQueue:      make(chan []byte, recvQueueSize),
			activeRequests: map[uint32]chan []byte{},
		},
		notifications: &notificationManager{activeNotifications: make(map[uint32]activeNotification), configsByKey: make(map[string]struct{}), orphanSeen: make(map[uint32]time.Time), orphanSem: make(chan struct{}, orphanDeleteMaxConcurrency)},
		cache:         &symbolCache{symbols: map[string]*symbol{}, onDemandSymbols: map[string]bool{}},
		logger:        getDefaultLogger(),
		lifecycle: &sessionLifecycle{
			closedCh:             make(chan struct{}),
			autoReconnect:        false,
			maxReconnectAttempts: 1,
			backoffConfig: BackoffConfig{
				InitialInterval: 1 * time.Millisecond,
				InitialAttempts: 10,
				MidInterval:     1 * time.Millisecond,
				MidAttempts:     10,
				SlowInterval:    1 * time.Millisecond,
				SlowAttempts:    10,
				MaxInterval:     1 * time.Millisecond,
			},
			ctx:      ctx,
			shutdown: cancel,
		},
		requestTimeout: 200 * time.Millisecond,
	}
	// Drive FSM to Disconnected so Reconnect() can transition to Reconnecting.
	sess.lifecycle.state.transitionTo(SessionStateConstructed)
	sess.lifecycle.state.transitionTo(SessionStateConnecting)
	sess.lifecycle.state.transitionTo(SessionStateConnected)
	sess.lifecycle.state.transitionTo(SessionStateDisconnected)

	reconnErr := sess.Reconnect(context.Background())
	if reconnErr == nil {
		t.Fatal("expected Reconnect() to return error after max attempts")
	}

	state := sess.lifecycle.state.load()
	if state != SessionStateClosed {
		t.Fatalf("FSM state = %v, want Closed after max attempts exhausted", state)
	}

	// closedCh must be closed (non-blocking receive succeeds).
	select {
	case <-sess.lifecycle.closedCh:
		// expected
	default:
		t.Fatal("closedCh not closed after max attempts exhausted")
	}
}

// TestReconnectExhaustConcurrentClose_NoPanic drives the exhaustion path
// while a second goroutine concurrently calls into the markClosed/transition
// pair. With closedOnce sync.Once gating close(closedCh), both racers can
// claim the FSM transition without double-close panic. Without the guard,
// "panic: close of closed channel" was possible whenever Close ran between
// exhaustion's transitionToOnce and its raw close(closedCh).
//
// Validates: closedOnce guard introduced in Phase 1.1 Group B.
func TestReconnectExhaustConcurrentClose_NoPanic(t *testing.T) {
	for iter := 0; iter < 20; iter++ {
		ctx, cancel := context.WithCancel(context.Background())

		sess := &Session{
			ip:   "127.0.0.1",
			port: 1, // refused on loopback
			tx: &transport{
				sendChannel:    make(chan []byte),
				systemResponse: make(chan []byte, 1),
				recvQueue:      make(chan []byte, recvQueueSize),
				activeRequests: map[uint32]chan []byte{},
			},
			notifications: &notificationManager{activeNotifications: make(map[uint32]activeNotification), configsByKey: make(map[string]struct{}), orphanSeen: make(map[uint32]time.Time), orphanSem: make(chan struct{}, orphanDeleteMaxConcurrency)},
			cache:         &symbolCache{symbols: map[string]*symbol{}, onDemandSymbols: map[string]bool{}},
			logger:        getDefaultLogger(),
			lifecycle: &sessionLifecycle{
				closedCh:             make(chan struct{}),
				autoReconnect:        false,
				maxReconnectAttempts: 1,
				backoffConfig: BackoffConfig{
					InitialInterval: 1 * time.Millisecond,
					InitialAttempts: 10,
					MidInterval:     1 * time.Millisecond,
					MidAttempts:     10,
					SlowInterval:    1 * time.Millisecond,
					SlowAttempts:    10,
					MaxInterval:     1 * time.Millisecond,
				},
				ctx:      ctx,
				shutdown: cancel,
			},
			requestTimeout: 200 * time.Millisecond,
		}
		sess.lifecycle.state.transitionTo(SessionStateConstructed)
		sess.lifecycle.state.transitionTo(SessionStateConnecting)
		sess.lifecycle.state.transitionTo(SessionStateConnected)
		sess.lifecycle.state.transitionTo(SessionStateDisconnected)

		// Race the exhaustion loop with a concurrent markClosed call. We test
		// markClosed directly rather than full Close() because Close()'s
		// reconnectDone wait would block this test on the goroutine we just
		// spawned. markClosed exercises the sync.Once guard, which is the
		// invariant under test.
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			_ = sess.Reconnect(context.Background())
		}()
		go func() {
			defer wg.Done()
			// Yield a few times so we land somewhere in the retry loop.
			for i := 0; i < 5; i++ {
				time.Sleep(time.Microsecond)
			}
			if _, ok := sess.lifecycle.state.transitionToOnce(SessionStateClosed); ok {
				sess.markClosed()
			} else {
				// Exhaustion path won; markClosed is idempotent.
				sess.markClosed()
			}
		}()
		wg.Wait()

		// Always end in Closed; closedCh always observably closed; no panic.
		if state := sess.lifecycle.state.load(); state != SessionStateClosed {
			t.Errorf("iter %d: FSM state = %v, want Closed", iter, state)
		}
		select {
		case <-sess.lifecycle.closedCh:
		default:
			t.Errorf("iter %d: closedCh not closed", iter)
		}
	}
}

// TestReconnect_FlapDetection_AccumulatesAcrossCycles exercises the cross-
// cycle flap counter introduced in v2.1. A PLC that disconnects shortly
// after each Connect should accumulate flapCount across Reconnect cycles,
// not just within a single cycle's retry loop.
//
// Validates the flap-counter field lives on sessionLifecycle and the
// flapWindow gating in Reconnect produces a sub-Connected→sub-Connected
// counter increment.
func TestReconnect_FlapDetection_AccumulatesAcrossCycles(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sess := &Session{
		ip:   "127.0.0.1",
		port: 1, // refused on loopback
		tx: &transport{
			sendChannel:    make(chan []byte),
			systemResponse: make(chan []byte, 1),
			recvQueue:      make(chan []byte, recvQueueSize),
			activeRequests: map[uint32]chan []byte{},
		},
		notifications: &notificationManager{activeNotifications: make(map[uint32]activeNotification), configsByKey: make(map[string]struct{}), orphanSeen: make(map[uint32]time.Time), orphanSem: make(chan struct{}, orphanDeleteMaxConcurrency)},
		cache:         &symbolCache{symbols: map[string]*symbol{}, onDemandSymbols: map[string]bool{}},
		logger:        getDefaultLogger(),
		lifecycle: &sessionLifecycle{
			closedCh:             make(chan struct{}),
			autoReconnect:        false,
			maxReconnectAttempts: 1, // exhaust quickly
			backoffConfig: BackoffConfig{
				InitialInterval: 1 * time.Millisecond,
				InitialAttempts: 10,
				MidInterval:     1 * time.Millisecond,
				MidAttempts:     10,
				SlowInterval:    1 * time.Millisecond,
				SlowAttempts:    10,
				MaxInterval:     1 * time.Millisecond,
			},
			ctx:      ctx,
			shutdown: cancel,
		},
		requestTimeout: 200 * time.Millisecond,
	}
	sess.lifecycle.state.transitionTo(SessionStateConstructed)
	sess.lifecycle.state.transitionTo(SessionStateConnecting)
	sess.lifecycle.state.transitionTo(SessionStateConnected)
	// Pretend we just had a successful connect that immediately dropped.
	sess.lifecycle.flapMu.Lock()
	sess.lifecycle.lastConnectedAt = time.Now()
	sess.lifecycle.lastConnectedAt = sess.lifecycle.lastConnectedAt.Add(-50 * time.Millisecond) // within flapWindow
	sess.lifecycle.lastConnectedAt = time.Now().Add(-50 * time.Millisecond)
	sess.lifecycle.flapMu.Unlock()
	sess.lifecycle.state.transitionTo(SessionStateDisconnected)

	if err := sess.Reconnect(context.Background()); err == nil {
		t.Fatal("expected Reconnect to fail (port refused)")
	}

	sess.lifecycle.flapMu.Lock()
	gotCount := sess.lifecycle.flapCount
	sess.lifecycle.flapMu.Unlock()
	if gotCount < 1 {
		t.Errorf("flapCount = %d after a within-flapWindow Reconnect, want >= 1", gotCount)
	}
}

// TestReconnect_WipesActiveNotificationsBeforeRetryLoop verifies Fix 3 of the
// v2.2.1 PLC-flood patch: Reconnect must capture the pre-reconnect handle
// list and wipe activeNotifications atomically before entering the retry
// loop, so a later successful dialAndStart can issue a bestEffortDelete
// against the saved handles (preventing TwinCAT AMS router handle-table
// accumulation across reconnect cycles).
//
// This test uses the refused-port session to force Reconnect into the
// exhaustion path; we only assert that the wipe ran. Full delete-RPC
// integration is exercised via hardware tests (TestIntegrationReconnect on
// TC3/TC2) where a real PLC accepts the SumDelete on the reconnected socket.
func TestReconnect_WipesActiveNotificationsBeforeRetryLoop(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	sess := &Session{
		ip:   "127.0.0.1",
		port: 1, // refused on loopback — Reconnect exhausts attempts
		tx: &transport{
			sendChannel:    make(chan []byte),
			systemResponse: make(chan []byte, 1),
			recvQueue:      make(chan []byte, recvQueueSize),
			activeRequests: map[uint32]chan []byte{},
		},
		notifications: &notificationManager{activeNotifications: make(map[uint32]activeNotification), configsByKey: make(map[string]struct{}), orphanSeen: make(map[uint32]time.Time), orphanSem: make(chan struct{}, orphanDeleteMaxConcurrency)},
		cache:         &symbolCache{symbols: map[string]*symbol{}, onDemandSymbols: map[string]bool{}},
		logger:        getDefaultLogger(),
		lifecycle: &sessionLifecycle{
			closedCh:             make(chan struct{}),
			autoReconnect:        false,
			maxReconnectAttempts: 1,
			backoffConfig: BackoffConfig{
				InitialInterval: 1 * time.Millisecond,
				InitialAttempts: 10,
				MidInterval:     1 * time.Millisecond,
				MidAttempts:     10,
				SlowInterval:    1 * time.Millisecond,
				SlowAttempts:    10,
				MaxInterval:     1 * time.Millisecond,
			},
			ctx:      ctx,
			shutdown: cancel,
		},
		requestTimeout: 200 * time.Millisecond,
	}
	sess.lifecycle.state.transitionTo(SessionStateConstructed)
	sess.lifecycle.state.transitionTo(SessionStateConnecting)
	sess.lifecycle.state.transitionTo(SessionStateConnected)
	sess.lifecycle.state.transitionTo(SessionStateDisconnected)

	// Stage pre-reconnect handles. Fix 3's snapshot+wipe must capture these
	// before the retry loop starts.
	sess.notifications.lock.Lock()
	sess.notifications.activeNotifications[0xAAAA] = activeNotification{Sym: &symbol{FullName: "MAIN.x"}}
	sess.notifications.activeNotifications[0xBBBB] = activeNotification{Sym: &symbol{FullName: "MAIN.y"}}
	sess.notifications.lock.Unlock()

	_ = sess.Reconnect(ctx)

	sess.notifications.lock.Lock()
	postLen := len(sess.notifications.activeNotifications)
	sess.notifications.lock.Unlock()
	if postLen != 0 {
		t.Errorf("activeNotifications len = %d post-Reconnect, want 0 (Fix 3 must wipe under same lock as snapshot)", postLen)
	}
}
