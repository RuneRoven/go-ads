package ads

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strconv"
	"sync"
	"time"
)

// ErrTransportClosed is returned by every Client method after the underlying
// TCP transport has been closed (Close called, drop detected, dial failed).
// Callers reconstruct a new *Client to re-establish.
var ErrTransportClosed = errors.New("ads: client transport closed")

// notificationHandler is the callback the recvWorker invokes when it decodes
// a DeviceNotification packet. Session installs its handleNotification here;
// raw Client consumers (web ADS browser, CLI tools) install their own.
type notificationHandler func(ctx context.Context, handle uint32, timestamp uint64, content []byte)

// Client is the Beckhoff-equivalent thin RPC layer. One TCP connection, raw
// AMS framing, request multiplexing via InvokeID, listen + transmit + recv
// worker goroutines. No cache, no FSM, no reconnect, no callbacks.
//
// Lifetime states are exactly two: alive (Dial succeeded, Close not yet
// called, transport not yet observed as dropped) and closed. Once closed,
// every public method returns ErrTransportClosed.
//
// Phase 5.a-types: struct fields declared. Methods (Dial/Close/Read/Write/
// notification dispatch) land in subsequent commits.
type Client struct {
	ip   string
	port int

	target AMSAddress
	source AMSAddress

	requestTimeout time.Duration
	logger         *slog.Logger

	tx *transport

	capabilities capabilities //nolint:unused // Phase 5.b.2 migrates capability state onto Client.

	// notify is invoked from recvWorker when a DeviceNotification packet
	// arrives. nil means raw Client (no dispatch). Session installs a
	// closure pointing at its handleNotification method.
	notifyMu sync.RWMutex
	notify   notificationHandler

	// Internal cancellation for the worker goroutines. Independent of any
	// caller context — Close cancels this to stop workers.
	ctx       context.Context
	cancel    context.CancelFunc
	waitGroup sync.WaitGroup

	closeOnce sync.Once
}

// Dial opens one TCP connection to ip:port, configures TCP keepalive, and
// returns a Client. The Client is the Beckhoff-equivalent thin RPC layer:
// see specs/09-fsm-design.md "Layer 2: Client (raw RPC)".
//
// Phase 5.a-types: this constructor establishes the TCP socket and sets up
// per-Client cancellation but does NOT yet spawn the listen / transmit /
// recvWorker goroutines (those still live on *Session in this phase). A
// Client constructed here can be Close()'d cleanly but cannot yet send
// RPCs; that capability lands in Phase 5.a-dial when the goroutine bodies
// migrate.
func Dial(
	ip string,
	port int,
	target, source AMSAddress,
	requestTimeout time.Duration,
	opts ...ClientOption,
) (*Client, error) {
	if requestTimeout <= 0 {
		requestTimeout = 5 * time.Second
	}
	c := &Client{
		ip:             ip,
		port:           port,
		target:         target,
		source:         source,
		requestTimeout: requestTimeout,
		logger:         slog.Default(),
		tx: &transport{
			sendChannel:    make(chan []byte),
			systemResponse: make(chan []byte),
			recvQueue:      make(chan []byte, recvQueueSize),
			activeRequests: map[uint32]chan []byte{},
		},
	}
	for _, opt := range opts {
		opt(c)
	}
	c.ctx, c.cancel = context.WithCancel(context.Background()) //nolint:gosec // cancel stored on c.cancel and called from Close.

	tcpConn, err := net.DialTimeout(
		"tcp",
		net.JoinHostPort(ip, strconv.Itoa(port)),
		requestTimeout,
	)
	if err != nil {
		c.cancel()
		return nil, fmt.Errorf("ads: dial %s:%d: %w", ip, port, err)
	}
	c.tx.connection = tcpConn
	configureKeepAlive(tcpConn)
	return c, nil
}

// Close cancels worker goroutines, closes the TCP connection, and waits for
// workers to exit. Idempotent: subsequent calls are no-ops returning nil.
//
// Phase 5.a-types: workers are not yet spawned here, so the WaitGroup is
// zero and Wait() returns immediately. The TCP socket is closed.
func (c *Client) Close() error {
	c.closeOnce.Do(func() {
		c.cancel()
		c.tx.connMu.Lock()
		if c.tx.connection != nil {
			_ = c.tx.connection.Close()
		}
		c.tx.connMu.Unlock()
		c.waitGroup.Wait()
	})
	return nil
}

// SetNotificationHandler installs (or replaces) the callback for inbound
// DeviceNotification packets. nil disables dispatch (packets dropped after
// a Debug log entry). Concurrent-safe; the recvWorker reads under RLock.
func (c *Client) SetNotificationHandler(fn notificationHandler) {
	c.notifyMu.Lock()
	c.notify = fn
	c.notifyMu.Unlock()
}
