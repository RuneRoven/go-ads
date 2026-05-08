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
//
// epoch is the unified generation counter (Phase 4): bumped on every
// transition INTO Connected and on every cache.symbols swap during Connected
// (user-driven LoadSymbols/LoadSymbolList/LoadDataTypes/RefreshSymbols).
// Replaces the previous cache.generation and reconnectGeneration counters.
// Retry helpers and TOCTOU re-checks load epoch at start, compare at retry
// point; any change means "something the caller cared about advanced."
// False-positive retries (e.g. a transition + a swap during one reconnect)
// are harmless.
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
// WARN. Used by call sites that already maintain the legacy boolean flags;
// the FSM call rides alongside as a shadow audit (Phase 1) until readers
// and writers swap over.
func (sess *Session) transitionState(want SessionState) {
	from, ok := sess.lifecycle.state.transitionTo(want)
	if !ok {
		sess.logger.Warn("FSM invalid transition (Phase 1 shadow — ignoring)",
			"from", from, "to", want)
		return
	}
	sess.logger.Log(context.Background(), LevelTrace, "FSM transition", "from", from, "to", want)
}

// isClosed reports whether the session has reached the terminal Closed
// state. The FSM is the only source of truth (Phase 3.b removed the
// legacy closed flag).
func (sess *Session) isClosed() bool {
	return sess.lifecycle.state.load() == SessionStateClosed
}

// isDisconnected reports whether the session has no live transport — either
// a drop has been detected (Disconnected) or a reconnect attempt is in
// progress (Reconnecting). Phase 2.b replacement for the legacy
// lifecycle.disconnected.Load() reader sites.
func (sess *Session) isDisconnected() bool {
	s := sess.lifecycle.state.load()
	return s == SessionStateDisconnected || s == SessionStateReconnecting
}

// isReconnecting reports whether a reconnect attempt is in flight.
// FSM-only (legacy reconnecting flag removed in Phase 3.c).
func (sess *Session) isReconnecting() bool {
	return sess.lifecycle.state.load() == SessionStateReconnecting
}

// epoch returns the unified generation counter for this session. See the
// sessionFSM doc for the full bump semantics. Retry helpers and TOCTOU
// re-checks use this to detect "something changed" since they captured.
func (sess *Session) epoch() uint64 {
	return sess.lifecycle.state.epoch.Load()
}

// bumpEpoch advances the counter for cache.symbols swaps that do NOT
// (yet) go through a Connected re-entry transition. Phase 6 wires the
// Reloading state for user-driven LoadSymbols/LoadSymbolList/RefreshSymbols
// and removes these manual bumps in favor of the transition-driven one.
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

// isTransportDown is the transport-level "no live socket" signal. Phase 5.b
// removed all callers when send/sendRequest moved onto *Client; Phase 5.c
// reintroduces it as the gate for Session-level clientRead/Write wrappers.
//
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
//
//nolint:unused // re-wired by Phase 5.c.
func (sess *Session) isTransportDown() bool {
	return sess.tx.disconnected.Load()
}
