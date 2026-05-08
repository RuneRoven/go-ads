package ads

import (
	"context"
	"sync"

	"go.uber.org/atomic"
)

// reconnector owns the connection lifecycle FSM state: ctx + shutdown for
// cancellation, waitGroup for goroutine tracking, reconnect/closed flags
// + their channels, the generation counter for stale-handle detection,
// and the configurable retry policy.
type reconnector struct {
	ctxMu     sync.RWMutex // protects ctx and shutdown against concurrent access during reconnect
	ctx       context.Context
	shutdown  context.CancelFunc
	waitGroup sync.WaitGroup

	reconnectMu   sync.Mutex // protects reconnectDone
	reconnectDone chan struct{}

	closedCh chan struct{}

	disconnected atomic.Bool

	// state is the explicit FSM state plus the unified epoch counter
	// (specs/09-fsm-design.md). FSM is the source of truth for closed and
	// reconnecting (Phase 3 removed the legacy flags). epoch replaces the
	// previous cache.generation and reconnectGeneration counters and bumps
	// on every Connected entry plus on user-driven cache swaps that don't
	// (yet) transition through Reloading.
	state sessionFSM

	autoReconnect              bool
	maxReconnectAttempts       int
	backoffConfig              BackoffConfig
	strictReconnect            bool
	strictReconnectMaxAttempts int
	strictReconnectFailures    int
}
