package ads

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
)

// SessionState is the explicit FSM state for a Session. See
// specs/09-fsm-design.md for the full state diagram, event taxonomy, and
// transition table.
//
// The FSM is the source of truth for closed and reconnecting concerns.
// The transport-down flag (disconnected on *transport) is a finer-grained
// signal than the FSM state — it flips false the instant dialAndStart
// reattaches a live socket, even while the FSM is still Reconnecting.
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
		// Connecting -> Disconnected covers Connect-time failure rollback:
		// dial error, route err, handshake err leave the FSM in a state
		// the caller can recover from via Reconnect, instead of forcing a
		// fresh NewSession.
		SessionStateDisconnected: {},
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
		// Allow Connect retry after a Connecting-time rollback into
		// Disconnected. Without this edge the recovered Session has only
		// Reconnecting available, which is auto-path-only.
		SessionStateConnecting: {},
		SessionStateClosed:     {},
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
//
// epoch is the unified generation counter: bumped on every transition INTO
// Connected and on every cache.symbols swap during Connected (user-driven
// LoadSymbols/LoadSymbolList/LoadDataTypes/RefreshSymbols). Retry helpers
// and TOCTOU re-checks load epoch at start, compare at retry point; any
// change means "something the caller cared about advanced." False-positive
// retries (e.g. a transition + a swap during one reconnect) are harmless.
type sessionFSM struct {
	mu    sync.Mutex
	value atomic.Uint32
	epoch atomic.Uint64
}

func (s *sessionFSM) load() SessionState { return SessionState(s.value.Load()) }

// transitionTo advances the state from its current value to want, returning
// the prior state and whether the transition was permitted by
// allowedTransitions. Holds s.mu for the duration so concurrent transitions
// serialize.
//
// Invalid transitions return ok=false; callers log but do not panic.
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
	if want == SessionStateConnected {
		s.epoch.Add(1)
	}
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
	if want == SessionStateConnected {
		s.epoch.Add(1)
	}
	return from, true
}

// transitionState advances the FSM state and logs invalid transitions at
// WARN.
func (sess *Session) transitionState(want SessionState) {
	from, ok := sess.lifecycle.state.transitionTo(want)
	if !ok {
		sess.logger.Warn("FSM invalid transition (ignoring)",
			"from", from, "to", want)
		return
	}
	sess.logger.Log(context.Background(), LevelTrace, "FSM transition", "from", from, "to", want)
}

// isClosed reports whether the session has reached the terminal Closed
// state.
func (sess *Session) isClosed() bool {
	return sess.lifecycle.state.load() == SessionStateClosed
}

// isDisconnected reports whether the session has no live transport — either
// a drop has been detected (Disconnected) or a reconnect attempt is in
// progress (Reconnecting).
func (sess *Session) isDisconnected() bool {
	s := sess.lifecycle.state.load()
	return s == SessionStateDisconnected || s == SessionStateReconnecting
}

// isReconnecting reports whether a reconnect attempt is in flight.
func (sess *Session) isReconnecting() bool {
	return sess.lifecycle.state.load() == SessionStateReconnecting
}

// epoch returns the unified generation counter for this session. See the
// sessionFSM doc for the full bump semantics. Retry helpers and TOCTOU
// re-checks use this to detect "something changed" since they captured.
func (sess *Session) epoch() uint64 {
	return sess.lifecycle.state.epoch.Load()
}

// bumpEpoch advances the counter for cache.symbols swaps that do NOT go
// through a Connected re-entry transition (e.g. user-driven
// LoadSymbols/LoadSymbolList/RefreshSymbols, AutoReload).
func (sess *Session) bumpEpoch() {
	sess.lifecycle.state.epoch.Add(1)
}

// waitForReconnect blocks until any in-flight reconnect completes (or the
// Session is closed). No-op if no reconnect is in flight.
//
// Used by Session-managed retry-helpers (readFromSymbolRetry etc.) before
// they decide whether to retry. Without this, a fast-failing transport-down
// error returns to the caller before the reconnect cycle has had a chance
// to advance epoch — the helper's "epoch changed?" check fires false and
// the user observes a transient failure that strict R-TX-005 says should
// have been transparent. With waitForReconnect inserted before the epoch
// re-check, the helper retries through the post-reconnect Client.
func (sess *Session) waitForReconnect() {
	if sess.isClosed() {
		return
	}
	sess.lifecycle.reconnectMu.Lock()
	ch := sess.lifecycle.reconnectDone
	sess.lifecycle.reconnectMu.Unlock()
	if ch == nil {
		return
	}
	select {
	case <-ch:
	case <-sess.lifecycle.closedCh:
	}
}

// isTransportDown reports whether the underlying TCP transport is not
// usable for sending. It reads the disconnected flag on *transport, which
// is flipped to false by dialAndStart immediately after a successful dial
// (i.e. inside the Reconnecting state, between dial and reload). The
// FSM-level isDisconnected() considers the entire Reconnecting state "no
// live transport," but during reload/resubscribe the transport IS alive —
// sendRequest needs to be able to use it to perform the reload itself.
//
//nolint:unused // re-wired by Session-level clientRead/Write wrappers.
func (sess *Session) isTransportDown() bool {
	return sess.tx.disconnected.Load()
}
