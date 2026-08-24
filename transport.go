package ads

import (
	"fmt"
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
	activeRequests    map[uint32]chan amsReply

	// disconnected reflects whether the underlying socket is usable for
	// sending. Flipped to false by Client.Dial after a successful TCP dial
	// (and by Session.dialAndStart on reconnect); flipped to true on
	// triggerReconnect, Reconnect entry, resetForRetry, and Close.
	// Lives on transport so the transport-down signal is co-located with the
	// socket it represents (rather than on a lifecycle struct that doesn't
	// own the connection).
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

// amsReply is one response handed back to the goroutine that issued the request.
//
// It carries the AMS header's ErrorCode alongside the payload because that field
// was previously discarded entirely: an AMS-level rejection (target port not found,
// no runtime, invalid NetID) arrives with a body that is not a valid response, and
// parsing it anyway produced fabricated diagnostics — a read of index group 0xF008
// reporting "0xF008: unknown error code", which is the group, not a code.
type amsReply struct {
	data   []byte
	amsErr ReturnCode
}

// payload returns the response body, or the AMS-level error the router reported.
//
// An AMS error means the request never reached a service that could answer it, so
// the body is not a response and must not be parsed. Reported with the code named:
// 0x06 target port not found is what a TwinCAT system in CONFIG answers for every
// request to a runtime port, and it is the difference between "the runtime is not
// running" and "this device is broken".
func (r amsReply) payload() ([]byte, error) {
	if r.amsErr != 0 {
		return nil, fmt.Errorf("AMS error from the router: %w", r.amsErr)
	}
	return r.data, nil
}
