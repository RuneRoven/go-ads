package ads

import (
	"context"
	"fmt"
	"sync"

	"go.uber.org/atomic"
)

// SessionState is the explicit FSM state for a Session. See
// specs/09-fsm-design.md for the full state diagram, event taxonomy, and
// transition table.
//
// Phase 3 makes the FSM the source of truth for the closed and reconnecting
// concerns. The legacy disconnected flag survives one more phase: it
// represents transport-down (flipped false by dialAndStart), which is a
// finer-grained signal than the FSM state. Phase 5 (Client extraction)
// moves it onto the transport type.
type SessionState uint32

const (
	SessionStateConstructed SessionState = iota
	SessionStateConnecting
	SessionStateConnected
	SessionStateReloading
	SessionStateDisconnected
	SessionStateReconnecting
	SessionStateClosed
)

func (s SessionState) String() string {
	switch s {
	case SessionStateConstructed:
		return "Constructed"
	case SessionStateConnecting:
		return "Connecting"
	case SessionStateConnected:
		return "Connected"
	case SessionStateReloading:
		return "Reloading"
	case SessionStateDisconnected:
		return "Disconnected"
	case SessionStateReconnecting:
		return "Reconnecting"
	case SessionStateClosed:
		return "Closed"
	default:
		return fmt.Sprintf("SessionState(%d)", uint32(s))
	}
}

// allowedTransitions encodes the transition table from specs/09-fsm-design.md.
// A nil entry means the from-state has no legal transitions out (terminal).
var allowedTransitions = map[SessionState]map[SessionState]struct{}{
	SessionStateConstructed: {
		SessionStateConnecting: {},
		SessionStateClosed:     {},
	},
	SessionStateConnecting: {
		SessionStateConnected:    {},
		SessionStateReconnecting: {},
		SessionStateClosed:       {},
	},
	SessionStateConnected: {
		SessionStateReloading:    {},
		SessionStateDisconnected: {},
		// Connected -> Reconnecting covers user-initiated force-reconnect
		// (public Reconnect() call without an intervening drop). Auto-path
		// reconnects still go Connected -> Disconnected -> Reconnecting.
		SessionStateReconnecting: {},
		SessionStateClosed:       {},
	},
	SessionStateReloading: {
		SessionStateConnected:    {},
		SessionStateDisconnected: {},
		SessionStateClosed:       {},
	},
	SessionStateDisconnected: {
		SessionStateReconnecting: {},
		SessionStateClosed:       {},
	},
	SessionStateReconnecting: {
		SessionStateConnected:    {},
		SessionStateReconnecting: {}, // self-loop on retry
		SessionStateClosed:       {},
	},
	SessionStateClosed: nil, // terminal
}

// sessionFSM wraps the atomic state field plus a transition mutex. The mutex
// serializes transitions; readers can Load lock-free.
type sessionFSM struct {
	mu    sync.Mutex
	value atomic.Uint32
}

func (s *sessionFSM) load() SessionState { return SessionState(s.value.Load()) }

// transitionTo advances the state from its current value to want, returning
// the prior state and whether the transition was permitted by
// allowedTransitions. Holds s.mu for the duration so concurrent transitions
// serialize.
//
// Phase 1 contract: invalid transitions return ok=false; callers log but
// do not panic. Once Phase 2/3 complete and the FSM is the source of truth,
// invalid transitions become programming errors (panic in dev / error in
// prod).
//
// Idempotent re-entry (from == want) returns ok=true without rechecking
// the table — harmless. Use transitionToOnce() if the caller needs the
// transition to be the FIRST one to land (Close gate, single-flight
// Reconnect launch).
func (s *sessionFSM) transitionTo(want SessionState) (from SessionState, ok bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	from = s.load()
	if from == want {
		return from, true
	}
	allowed, exists := allowedTransitions[from]
	if !exists || allowed == nil {
		return from, false
	}
	if _, ok := allowed[want]; !ok {
		return from, false
	}
	s.value.Store(uint32(want))
	return from, true
}

// transitionToOnce is like transitionTo but returns ok=false on idempotent
// re-entry (from == want). Use this when the caller needs to know it is
// the FIRST goroutine to land the transition — e.g. Close (only one
// shutdown sequence runs) or Reconnect (only one retry loop launches).
//
// Concurrent callers race on s.mu; exactly one observes the from-state
// other than want and returns ok=true. Subsequent callers see from==want
// and return ok=false.
func (s *sessionFSM) transitionToOnce(want SessionState) (from SessionState, ok bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	from = s.load()
	if from == want {
		return from, false
	}
	allowed, exists := allowedTransitions[from]
	if !exists || allowed == nil {
		return from, false
	}
	if _, ok := allowed[want]; !ok {
		return from, false
	}
	s.value.Store(uint32(want))
	return from, true
}

// transitionState advances the FSM state and logs invalid transitions at
// WARN. Used by call sites that already maintain the legacy boolean flags;
// the FSM call rides alongside as a shadow audit (Phase 1) until readers
// and writers swap over.
func (conn *Session) transitionState(want SessionState) {
	from, ok := conn.lifecycle.state.transitionTo(want)
	if !ok {
		conn.logger.Warn("FSM invalid transition (Phase 1 shadow — ignoring)",
			"from", from, "to", want)
		return
	}
	conn.logger.Log(context.Background(), LevelTrace, "FSM transition", "from", from, "to", want)
}

// isClosed reports whether the session has reached the terminal Closed
// state. The FSM is the only source of truth (Phase 3.b removed the
// legacy closed flag).
func (conn *Session) isClosed() bool {
	return conn.lifecycle.state.load() == SessionStateClosed
}

// isDisconnected reports whether the session has no live transport — either
// a drop has been detected (Disconnected) or a reconnect attempt is in
// progress (Reconnecting). Phase 2.b replacement for the legacy
// lifecycle.disconnected.Load() reader sites.
func (conn *Session) isDisconnected() bool {
	s := conn.lifecycle.state.load()
	return s == SessionStateDisconnected || s == SessionStateReconnecting
}

// isReconnecting reports whether a reconnect attempt is in flight.
// FSM-only (legacy reconnecting flag removed in Phase 3.c).
func (conn *Session) isReconnecting() bool {
	return conn.lifecycle.state.load() == SessionStateReconnecting
}

// isTransportDown reports whether the underlying TCP transport is not
// usable for sending. It reads the legacy disconnected flag, which is
// flipped to false by dialAndStart immediately after a successful dial
// (i.e. inside the Reconnecting state, between dial and reload). The
// FSM-level isDisconnected() considers the entire Reconnecting state
// "no live transport," but during reload/resubscribe the transport IS
// alive — sendRequest needs to be able to use it to perform the reload
// itself.
//
// Phase 2 keeps this on the legacy flag. Phase 5 (Client extraction)
// will move this signal onto the transport type, where it belongs.
func (conn *Session) isTransportDown() bool {
	return conn.lifecycle.disconnected.Load()
}
