package ads

import (
	"net"
	"sync"
	"sync/atomic"
)

// transport owns the TCP socket, the per-invoke request multiplexing,
// and the inbound/outbound goroutine channels used by listen and transmit.
//
// recvQueue + recvWorkers form a bounded worker pool that processes
// non-system inbound packets (notifications + command responses). Pre-fix
// listen spawned a goroutine per packet, which under a misbehaving PLC or
// adversarial network could spawn unbounded goroutines. The bounded pool
// caps concurrent decode+dispatch work; overflow is dropped with a Warn log.
type transport struct {
	connMu         sync.Mutex   // protects connection field against concurrent Close/Reconnect
	chanMu         sync.RWMutex // protects sendChannel and systemResponse against concurrent access during reconnect
	connection     net.Conn
	sendChannel    chan []byte
	systemResponse chan []byte
	recvQueue      chan []byte // bounded inbound packet queue feeding recvWorkers

	currentRequest    atomic.Uint32
	activeRequestLock sync.Mutex // protects activeRequests against concurrent map access
	activeRequests    map[uint32]chan []byte

	// disconnected reflects whether the underlying socket is usable for
	// sending. Flipped to false by Client.Dial after a successful TCP dial
	// (and by Session.dialAndStart on reconnect); flipped to true on
	// triggerReconnect, Reconnect entry, resetForRetry, and Close.
	// Phase 5.c relocated the flag from sessionLifecycle to transport so the
	// transport-down signal lives next to the socket it represents.
	disconnected atomic.Bool
}

// recvWorkerCount is the number of goroutines consuming recvQueue. Each
// worker handles one packet at a time end-to-end. Sizing: high enough that
// notification storms don't bottleneck on cache.lock contention, low enough
// that a runaway PLC can't allocate goroutines without bound.
const recvWorkerCount = 16

// recvQueueSize is the buffer between listen and recvWorkers. Beyond this,
// listen drops packets with a Warn log.
const recvQueueSize = 256
