package ads

import (
	"net"
	"sync"

	"go.uber.org/atomic"
)

// transport owns the TCP socket, the per-invoke request multiplexing,
// and the inbound/outbound goroutine channels used by listen and transmit.
type transport struct {
	connMu         sync.Mutex   // protects connection field against concurrent Close/Reconnect
	chanMu         sync.RWMutex // protects sendChannel and systemResponse against concurrent access during reconnect
	connection     net.Conn
	sendChannel    chan []byte
	systemResponse chan []byte

	currentRequest    atomic.Uint32
	activeRequestLock sync.Mutex // protects activeRequests against concurrent map access
	activeRequests    map[uint32]chan []byte
}
