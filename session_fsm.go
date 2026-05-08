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
// During Phase 1 of the FSM rollout the state field is a SHADOW: it is
// maintained alongside the legacy boolean flags (lifecycle.closed,
// lifecycle.disconnected, lifecycle.reconnecting) but no code path reads
// from it for behavior decisions. Phase 2 swaps readers; Phase 3 removes
// the flags.
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
// the table — harmless.
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
