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
	reconnecting  atomic.Bool
	reconnectDone chan struct{}

	closed   atomic.Bool
	closedCh chan struct{}

	disconnected        atomic.Bool
	reconnectGeneration atomic.Uint64

	autoReconnect              bool
	maxReconnectAttempts       int
	backoffConfig              BackoffConfig
	strictReconnect            bool
	strictReconnectMaxAttempts int
	strictReconnectFailures    int
}
