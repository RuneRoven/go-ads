package ads

import (
	"sync"
	"testing"
)

// TestFSM_AllowedTransitions table-pins every legal and illegal (from, to)
// transition in the FSM. The spec lives in specs/09-fsm-design.md; this
// test guards against merge-conflict resolution that drops a required edge
// or adds an illegal one (e.g. Closed→Connecting).
//
// Regression scope: any change to allowedTransitions must update this table
// in lockstep; a green test against a quiet diff is the signal that the
// edge set matches the documented contract.
func TestFSM_AllowedTransitions(t *testing.T) {
	allStates := []SessionState{
		SessionStateConstructed,
		SessionStateConnecting,
		SessionStateConnected,
		SessionStateReloading,
		SessionStateDisconnected,
		SessionStateReconnecting,
		SessionStateClosed,
	}

	// Legal edges. Keep in sync with allowedTransitions in session_fsm.go.
	legal := map[SessionState]map[SessionState]bool{
		SessionStateConstructed: {
			SessionStateConnecting: true,
			SessionStateClosed:     true,
		},
		SessionStateConnecting: {
			SessionStateConnected:    true,
			SessionStateReconnecting: true,
			SessionStateDisconnected: true,
			SessionStateClosed:       true,
		},
		SessionStateConnected: {
			SessionStateReloading:    true,
			SessionStateDisconnected: true,
			SessionStateReconnecting: true,
			SessionStateClosed:       true,
		},
		SessionStateReloading: {
			SessionStateConnected:    true,
			SessionStateDisconnected: true,
			SessionStateClosed:       true,
		},
		SessionStateDisconnected: {
			SessionStateReconnecting: true,
			SessionStateConnecting:   true,
			SessionStateClosed:       true,
		},
		SessionStateReconnecting: {
			SessionStateConnected:    true,
			SessionStateReconnecting: true, // self-loop on retry
			SessionStateClosed:       true,
		},
		SessionStateClosed: {}, // terminal — nothing legal out
	}

	for _, from := range allStates {
		for _, to := range allStates {
			from, to := from, to
			t.Run(from.String()+"_to_"+to.String(), func(t *testing.T) {
				fsm := &sessionFSM{}
				fsm.value.Store(uint32(from))
				wantOK := legal[from][to]
				// Idempotent re-entry on transitionTo always returns ok=true.
				if from == to {
					wantOK = true
				}
				_, gotOK := fsm.transitionTo(to)
				if gotOK != wantOK {
					t.Errorf("transitionTo(%v) from %v: ok=%v, want %v", to, from, gotOK, wantOK)
				}
				if wantOK && from != to {
					if got := fsm.load(); got != to {
						t.Errorf("after transitionTo(%v) from %v: load()=%v, want %v", to, from, got, to)
					}
				}
			})
		}
	}
}

// TestFSM_TransitionToOnce_RejectsIdempotent verifies the single-flight
// gate: from==want returns ok=false so Close / Reconnect launches can use
// the return value to detect "first winner."
func TestFSM_TransitionToOnce_RejectsIdempotent(t *testing.T) {
	fsm := &sessionFSM{}
	fsm.value.Store(uint32(SessionStateConnected))
	if _, ok := fsm.transitionToOnce(SessionStateConnected); ok {
		t.Errorf("transitionToOnce(Connected) from Connected: ok=true, want false (idempotent must reject)")
	}
}

// TestFSM_TransitionToOnce_FirstWinnerOnly: under N concurrent callers
// targeting the same terminal-style transition, exactly one observes ok=true.
// Guards the close-once / exhaust-once contract.
func TestFSM_TransitionToOnce_FirstWinnerOnly(t *testing.T) {
	fsm := &sessionFSM{}
	fsm.value.Store(uint32(SessionStateConnected))

	const goroutines = 50
	var wg sync.WaitGroup
	wins := make(chan bool, goroutines)
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			_, ok := fsm.transitionToOnce(SessionStateClosed)
			wins <- ok
		}()
	}
	wg.Wait()
	close(wins)
	var winners int
	for ok := range wins {
		if ok {
			winners++
		}
	}
	if winners != 1 {
		t.Errorf("concurrent transitionToOnce(Closed): %d winners, want exactly 1", winners)
	}
}

// TestFSM_EpochBumpsOnlyOnConnected verifies epoch increments precisely at
// the transition INTO Connected, not on other transitions. Retry helpers
// rely on epoch as the "something the caller cared about advanced" signal.
func TestFSM_EpochBumpsOnlyOnConnected(t *testing.T) {
	fsm := &sessionFSM{}
	fsm.value.Store(uint32(SessionStateConnecting))

	before := fsm.epoch.Load()
	if _, ok := fsm.transitionTo(SessionStateConnected); !ok {
		t.Fatalf("Connecting->Connected transition should succeed")
	}
	if got := fsm.epoch.Load(); got != before+1 {
		t.Errorf("epoch after ->Connected: %d, want %d", got, before+1)
	}

	// A non-Connected transition must not bump.
	beforeNonConn := fsm.epoch.Load()
	if _, ok := fsm.transitionTo(SessionStateDisconnected); !ok {
		t.Fatalf("Connected->Disconnected transition should succeed")
	}
	if got := fsm.epoch.Load(); got != beforeNonConn {
		t.Errorf("epoch after ->Disconnected: %d, want %d (no bump)", got, beforeNonConn)
	}
}
