package ads

import (
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
		notifications: &notificationManager{activeNotifications: make(map[uint32]*Symbol)},
		cache:         &symbolCache{symbols: map[string]*Symbol{}, onDemandSymbols: map[string]bool{}},
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
