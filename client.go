// Package ads is a pure-Go client for the Beckhoff TwinCAT ADS protocol.
//
// The package exposes two layers:
//
//   - Client (this file): a thin Beckhoff-equivalent RPC layer. One TCP
//     connection, raw AMS framing, request multiplexing, no cache, no
//     reconnect, no notification persistence. Construct via Dial; once the
//     transport drops, every subsequent method returns ErrTransportClosed
//     and the caller reconstructs a new Client. Suitable for one-shot
//     consumers (CLI tools, web ADS browsers).
//
//   - Session (session.go): a managed wrapper that adds the symbol cache,
//     name-based read/write, persistent notifications with auto-resubscribe
//     after a reconnect, auto-reconnect with backoff, lifecycle callbacks,
//     and an explicit FSM (docs/archive/specs/09-fsm-design.md). Construct via
//     NewSession + Connect.
//
// Session does NOT embed *Client; pick a layer at construction time. Raw
// methods (Read, Write, Sum*, AddDeviceNotification, ReadProcess*, etc.)
// live on *Client only. Cache-aware methods (ReadFromSymbol,
// AddSymbolNotification, LoadSymbols, …) live on *Session only.
package ads

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// ErrTransportClosed is returned by every Client method after the underlying
// TCP transport has been closed (Close called, drop detected, dial failed).
// Callers reconstruct a new *Client to re-establish.
var ErrTransportClosed = errors.New("ads: client transport closed")

// isLikelyMissingRoute returns true if err indicates a likely missing-AMS-route
// condition (PLC closed the TCP connection because no route exists for our
// NetID). Detects wrapped io.EOF and ECONNRESET via the standard
// errors.Is/As mechanism. Used by listen to add a hint to the
// "transport down" log line.
func isLikelyMissingRoute(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, io.EOF) {
		return true
	}
	var netErr *net.OpError
	if errors.As(err, &netErr) {
		if errors.Is(netErr.Err, syscall.ECONNRESET) {
			return true
		}
	}
	return errors.Is(err, syscall.ECONNRESET)
}

// NotificationHandler is the callback the recvWorker invokes when it decodes
// a DeviceNotification packet. Session installs its handleNotification here;
// raw Client consumers (web ADS browser, CLI tools, AMS routers) install
// their own. ctx is the Client's internal worker context — observe Done()
// to abort long-running handler work on Close. handle, timestamp, content
// are the parsed notification fields (handle = PLC-assigned ID, timestamp =
// Windows FILETIME, content = raw payload bytes).
type NotificationHandler func(ctx context.Context, handle uint32, timestamp uint64, content []byte)

// Client is the Beckhoff-equivalent thin RPC layer. One TCP connection, raw
// AMS framing, request multiplexing via InvokeID, listen + transmit + recv
// worker goroutines. No cache, no FSM, no reconnect, no callbacks.
//
// Lifetime states are exactly two: alive (Dial succeeded, Close not yet
// called, transport not yet observed as dropped) and closed. Once closed,
// every public method returns ErrTransportClosed.
type Client struct {
	ip   string
	port int

	target AMSAddress
	source AMSAddress

	requestTimeout time.Duration
	logger         *slog.Logger

	tx *transport

	capabilities capabilities //nolint:unused // capability state lives here, accessed via Client methods.

	// notify is invoked from recvWorker when a DeviceNotification packet
	// arrives. nil means raw Client (no dispatch). Session installs a
	// closure pointing at its handleNotification method.
	notifyMu sync.RWMutex
	notify   NotificationHandler

	// ondrop is invoked once on listen / transmitWorker error. nil means
	// raw Client (no auto-recovery — caller observes via ErrTransportClosed
	// from subsequent RPCs). Session installs s.triggerReconnect.
	ondropMu sync.RWMutex
	ondrop   func()

	// handshaking counts route probe / registration regions in flight; see
	// beginHandshake for why transport faults are demoted to Debug then. A
	// counter rather than a flag so overlapping regions cannot have the inner
	// one re-enable ERROR logging while the outer is still running — the same
	// reason subscribeInFlight is a counter.
	handshaking atomic.Int64

	// dropped is closed when THIS client's connection is known to be gone.
	// disconnected stops new requests; this releases the ones already blocked
	// on a response that will never come, which a flag cannot do.
	//
	// Per-Client, not per-transport, and immutable for the client's lifetime:
	// a Session reuses one transport across reconnects but allocates a fresh
	// Client each time, so "this connection died" is a client-scoped fact. Held
	// on the transport it was signalled by a stale client's listen goroutine
	// after the replacement had already re-armed it, which killed every request
	// on the new connection.
	dropped  chan struct{}
	dropOnce sync.Once

	// Internal cancellation for the worker goroutines. Independent of any
	// caller context — Close cancels this to stop workers.
	ctx       context.Context
	cancel    context.CancelFunc
	waitGroup sync.WaitGroup

	closeOnce sync.Once
}

// markDropped releases every request waiting on this client's connection.
// Idempotent: a drop can be observed by listen and the transmit worker both.
func (c *Client) markDropped() {
	c.dropOnce.Do(func() {
		if c.dropped != nil {
			close(c.dropped)
		}
	})
}

// Dial opens one TCP connection to ip:port, configures TCP keepalive, and
// spawns the listen / transmit / recvWorker goroutines. Returns a usable
// Client. See docs/archive/specs/09-fsm-design.md "Layer 2: Client (raw RPC)".
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
		dropped:        make(chan struct{}),
		tx: &transport{
			sendChannel:    make(chan []byte),
			systemResponse: make(chan []byte, 1),
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
	c.startWorkers()
	return c, nil
}

// Close cancels worker goroutines, closes the TCP connection, and waits for
// workers to exit. Idempotent: subsequent calls are no-ops returning nil.
// Sets tx.disconnected so any subsequent RPC method returns
// ErrTransportClosed immediately.
func (c *Client) Close() error {
	c.closeOnce.Do(func() {
		c.tx.disconnected.Store(true)
		c.markDropped()
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

// ClientOption configures optional construction parameters for Dial.
type ClientOption func(*Client)

// WithClientLogger sets the slog.Logger for a Client. Nil is ignored.
func WithClientLogger(logger *slog.Logger) ClientOption {
	return func(c *Client) {
		if logger != nil {
			c.logger = logger
		}
	}
}

// WithClientRequestTimeout overrides the per-request and dial timeout.
// Values <= 0 are ignored (the default of 5s applies).
func WithClientRequestTimeout(d time.Duration) ClientOption {
	return func(c *Client) {
		if d > 0 {
			c.requestTimeout = d
		}
	}
}

// WithNotificationHandler installs a callback for inbound DeviceNotification
// packets. Session installs its own handler internally; raw Client consumers
// (CLI, web ADS browser) install their own to receive notifications, or
// leave nil to drop them silently.
func WithNotificationHandler(fn NotificationHandler) ClientOption {
	return func(c *Client) {
		c.notify = fn
	}
}

// SetNotificationHandler installs (or replaces) the callback for inbound
// DeviceNotification packets. nil disables dispatch (packets dropped after
// a Debug log entry). Concurrent-safe; the recvWorker reads under RLock.
func (c *Client) SetNotificationHandler(fn NotificationHandler) {
	c.notifyMu.Lock()
	c.notify = fn
	c.notifyMu.Unlock()
}

// WithOnDrop registers a callback fired on unexpected transport drop.
// See SetOnDrop for the runtime equivalent.
func WithOnDrop(fn func()) ClientOption {
	return func(c *Client) {
		c.SetOnDrop(fn)
	}
}

// SetOnDrop registers a callback fired on unexpected transport drop.
// Prefer WithOnDrop at construction time.
func (c *Client) SetOnDrop(fn func()) {
	c.ondropMu.Lock()
	c.ondrop = fn
	c.ondropMu.Unlock()
}

// beginHandshake marks a route probe / registration region as in flight. During the handshake a dropped connection and an unanswered request
// are expected states, not faults: the normal cold-start flow is probe → PLC
// rejects an unknown NetID → register route → redial → probe again. Logging
// those at ERROR misreports a connect that is still progressing, and
// downstream log-based health checks (umh-core's IsLogsFine fails a data-flow
// component on any level=error line in its recent window) hold the component
// in a starting state even though the PLC is connected and streaming.
//
// Errors are still returned to the caller unchanged — only the log level moves.
func (c *Client) beginHandshake() {
	c.handshaking.Add(1)
}

// endHandshake closes a region opened by beginHandshake. Pairs must match; a
// count that never returns to zero would silence real faults for the life of
// the client, so callers defer this immediately.
func (c *Client) endHandshake() {
	if c.handshaking.Add(-1) < 0 {
		// Unbalanced: clamp rather than leave it negative, which would read as
		// "not handshaking" only by accident.
		c.handshaking.Store(0)
	}
}

// transportFaultLevel returns the level to log a transport fault at: Debug
// while a handshake is in flight, Error otherwise.
//
// Use it for every TRANSPORT fault — the link went away, a request went
// unanswered, a write failed — because all of those are expected states of the
// probe → register → redial cold start. Do NOT use it for protocol or
// programming faults (header/body parse, packet exceeds the sanity limit,
// binary.Write failure): a handshake never legitimately produces those, and
// demoting them would hide corruption. One un-gated transport site is enough to
// re-trip a downstream log-based health check, so new fault paths need this
// distinction made deliberately.
func (c *Client) transportFaultLevel() slog.Level {
	if c.handshaking.Load() > 0 {
		return slog.LevelDebug
	}
	return slog.LevelError
}

func (c *Client) callOnDrop() {
	// Release every request on THIS client — the ones already blocked as well as
	// any issued later — so they fail fast with ErrTransportClosed instead of
	// waiting out a timeout on a dead socket. Measured cost of not doing this: a
	// 40-symbol notification batch that lost the link at symbol 3 took 3m10s to
	// return, holding the subscribe window (and so disabling the orphan reaper)
	// for all of it.
	//
	// Deliberately does NOT touch tx.disconnected. That flag lives on the
	// transport, which a Session reuses across reconnects, and Session already
	// owns it (triggerReconnect, Reconnect, resetForRetry, Close). Setting it
	// here let a stale client's listen goroutine flip it back to true after the
	// replacement had cleared it, which made every probe on the new connection
	// fail instantly with ErrTransportClosed — route registration could then
	// never complete.
	c.markDropped()
	c.ondropMu.RLock()
	fn := c.ondrop
	c.ondropMu.RUnlock()
	if fn != nil {
		fn()
	}
}

// startWorkers spawns the listen, transmit, and recvWorker goroutines.
// Called by Dial after the TCP socket is established, and by Session at
// every successful dial / redial. Each goroutine ends on c.ctx.Done() or
// transport-level error.
func (c *Client) startWorkers() {
	c.waitGroup.Add(2 + recvWorkerCount)
	go c.listen()
	go c.transmitWorker()
	for i := 0; i < recvWorkerCount; i++ {
		go c.recvWorker()
	}
}

func (c *Client) listen() {
	defer c.waitGroup.Done()
	reader := bufio.NewReader(c.tx.connection)
	const maxAMSPacket = 4 * 1024 * 1024
	var hdrBytes [6]byte
	for {
		select {
		case <-c.ctx.Done():
			c.logger.Info("exit listen")
			return
		default:
		}
		if _, err := io.ReadFull(reader, hdrBytes[:]); err != nil {
			select {
			case <-c.ctx.Done():
				return
			default:
			}
			hint := ""
			if isLikelyMissingRoute(err) {
				// A reset straight after a successful TCP connect has three
				// plausible causes, and naming only the route one has sent
				// people down the wrong path for a mistyped target NetID.
				// Report all three, and log both ends of the addressing so the
				// reader can check them without reconstructing the config.
				hint = "a reset right after TCP connect means one of: " +
					"the target NetID does not exist on this PLC, " +
					"the route credentials were rejected, " +
					"or the AMS port addresses no running runtime (851 for TC3, 801 for TC2)"
			}
			if hint != "" {
				c.logger.Log(c.ctx, c.transportFaultLevel(), "PLC closed connection, transport down",
					"error", err, "hint", hint,
					"sourceNetID", c.source.NetIDString(),
					"targetNetID", c.target.NetIDString(),
					"targetPort", c.target.Port)
			} else {
				c.logger.Log(c.ctx, c.transportFaultLevel(), "listen read error, transport down", "error", err)
			}
			c.callOnDrop()
			return
		}
		var tcpHeader amsTCPHeader
		if err := binary.Read(bytes.NewReader(hdrBytes[:]), binary.LittleEndian, &tcpHeader); err != nil {
			c.logger.Error("listen header decode error, transport down", "error", err)
			c.callOnDrop()
			return
		}
		if tcpHeader.Length > maxAMSPacket {
			c.logger.Error("AMS packet length exceeds sanity limit, transport down",
				"length", tcpHeader.Length)
			c.callOnDrop()
			return
		}
		data := make([]byte, tcpHeader.Length)
		select {
		case <-c.ctx.Done():
			return
		default:
		}
		if _, err := io.ReadFull(reader, data); err != nil {
			select {
			case <-c.ctx.Done():
				return
			default:
			}
			c.logger.Log(c.ctx, c.transportFaultLevel(), "listen body read error, transport down", "error", err)
			c.callOnDrop()
			return
		}
		if tcpHeader.System > 0 {
			select {
			case c.tx.systemResponse <- data:
			case <-c.ctx.Done():
				return
			}
		} else {
			select {
			case c.tx.recvQueue <- data:
			case <-c.ctx.Done():
				return
			default:
				c.logger.Warn("recvQueue full, dropping inbound packet (PLC overrun or slow handler)",
					"queue_size", recvQueueSize,
					"workers", recvWorkerCount,
					"packet_bytes", len(data))
			}
		}
	}
}

func (c *Client) recvWorker() {
	defer c.waitGroup.Done()
	for {
		select {
		case <-c.ctx.Done():
			return
		case data, ok := <-c.tx.recvQueue:
			if !ok {
				return
			}
			c.handleReceive(c.ctx, data)
		}
	}
}

func (c *Client) handleReceive(ctx context.Context, data []byte) {
	c.logger.Log(context.Background(), LevelTrace, "in read")
	if len(data) < 32 {
		c.logger.Error("header too short")
		return
	}
	buff := bytes.NewBuffer(data)
	header := AMSHeader{}
	if err := binary.Read(buff, binary.LittleEndian, &header); err != nil {
		c.logger.Error("Error parsing header", "error", err)
		return
	}
	c.logger.Log(context.Background(), LevelTrace, "header info", "header", header)
	adsData := data[32:]
	if len(adsData) != int(header.Length) {
		c.logger.Error("Error parsing body")
		return
	}
	switch header.Command {
	case CommandIDDeviceNotification:
		if err := c.deviceNotification(ctx, adsData); err != nil {
			c.logger.Error("device notification decode failed", "error", err)
		}
	default:
		c.logger.Log(context.Background(), LevelTrace, "default receive")
		c.tx.activeRequestLock.Lock()
		response, ok := c.tx.activeRequests[header.InvokeID]
		c.tx.activeRequestLock.Unlock()
		if ok {
			select {
			case <-ctx.Done():
				c.logger.Info("receive channel timed out",
					"id", header.InvokeID, "command", header.Command)
				return
			case response <- adsData:
				c.logger.Log(context.Background(), LevelTrace, "Successfully delivered answer",
					"id", header.InvokeID, "command", header.Command)
			}
		} else {
			// Stale invokeID. Expected during reconnect cleanup or shutdown:
			// activeRequests was cleared, late PLC responses arrive after.
			// Always Debug — Client has no FSM context to distinguish more
			// precisely; production debugging uses the InvokeID + command
			// pair to spot true protocol bugs.
			c.logger.Debug("received packet with unknown invokeID",
				"invokeID", header.InvokeID,
				"command", header.Command)
		}
	}
}

func (c *Client) transmitWorker() {
	defer c.waitGroup.Done()
	writer := bufio.NewWriter(c.tx.connection)
	for {
		select {
		case <-c.ctx.Done():
			c.logger.Debug("Exit transmitWorker")
			return
		case data := <-c.tx.sendChannel:
			c.logger.Log(context.Background(), LevelTrace, fmt.Sprintf("Sending %d bytes", len(data)))
			if _, err := writer.Write(data); err != nil {
				c.logger.Log(c.ctx, c.transportFaultLevel(), "error sending data on conn, transport down", "error", err)
				c.callOnDrop()
				return
			}
			if err := writer.Flush(); err != nil {
				c.logger.Log(c.ctx, c.transportFaultLevel(), "error flushing data on conn, transport down", "error", err)
				c.callOnDrop()
				return
			}
		}
	}
}

func (c *Client) deviceNotification(ctx context.Context, in []byte) error {
	var stream NotificationStream
	var header StampHeader
	var sample NotificationSample
	var content []byte
	data := bytes.NewBuffer(in)
	if err := binary.Read(data, binary.LittleEndian, &stream); err != nil {
		return fmt.Errorf("unable to read notification: %w", err)
	}
	for i := uint32(0); i < stream.Stamps; i++ {
		if err := binary.Read(data, binary.LittleEndian, &header); err != nil {
			return fmt.Errorf("error reading stamp header: %w", err)
		}
		for j := uint32(0); j < header.Samples; j++ {
			if err := binary.Read(data, binary.LittleEndian, &sample); err != nil {
				return fmt.Errorf("error reading notification sample: %w", err)
			}
			if sample.Size > uint32(data.Len()) {
				return fmt.Errorf("notification sample size %d exceeds remaining data %d",
					sample.Size, data.Len())
			}
			content = make([]byte, sample.Size)
			n, err := data.Read(content)
			if err != nil {
				return fmt.Errorf("error reading notification content: %w", err)
			}
			if n != int(sample.Size) {
				return fmt.Errorf("short read on notification content: got %d of %d bytes",
					n, sample.Size)
			}
			c.dispatchNotification(ctx, sample.Handle, header.Timestamp, content)
		}
	}
	return nil
}

// ReleaseHandle releases a symbol handle previously acquired via
// GetHandleByName. Wraps Write to GroupSymbolReleaseHandle so the
// Beckhoff-equivalent surface includes a symmetric release primitive.
func (c *Client) ReleaseHandle(ctx context.Context, handle uint32) error {
	handleBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(handleBytes, handle)
	return c.Write(ctx, uint32(GroupSymbolReleaseHandle), 0, handleBytes)
}

// send is the local-mode handshake primitive. NOT safe for concurrent use —
// it consumes from the shared systemResponse channel. Used by Session for
// the local AMS handshake during Connect/Reconnect.
func (c *Client) send(data []byte) ([]byte, error) {
	c.tx.currentRequest.Add(1)
	c.tx.chanMu.RLock()
	sendCh := c.tx.sendChannel
	sysCh := c.tx.systemResponse
	c.tx.chanMu.RUnlock()
	dropped := c.dropped
	ctx, cancel := context.WithTimeout(c.ctx, c.requestTimeout)
	defer cancel()
	select {
	case <-ctx.Done():
		return nil, fmt.Errorf("send aborted, context canceled: %w", ctx.Err())
	case <-dropped:
		return nil, ErrTransportClosed
	case sendCh <- data:
	}
	select {
	case <-ctx.Done():
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			err := fmt.Errorf("request aborted, deadline exceeded: %w", ctx.Err())
			c.logger.Log(ctx, c.transportFaultLevel(), "send aborted due to timeout", "error", err)
			return nil, err
		}
		err := fmt.Errorf("request aborted, shutdown initiated: %w", ctx.Err())
		c.logger.Log(ctx, c.transportFaultLevel(), "send aborted due to shutdown", "error", err)
		return nil, err
	case <-dropped:
		return nil, ErrTransportClosed
	case response := <-sysCh:
		return response, nil
	}
}

// sendRequest is the single-shot RPC primitive used by every Client RPC
// method. Encodes the AMS frame, registers a per-invoke response channel,
// pushes the frame to the transmit worker, and waits for the response or
// context cancel / timeout.
//
// The caller's ctx is merged with c.requestTimeout via context.WithTimeout;
// whichever fires first cancels the wait. Pass context.Background() to
// preserve the v2.1 "timeout-only" semantic.
//
// Returns ErrTransportClosed immediately if the transport is known dead
// (Close called or drop detected). Otherwise no retry — drops mid-flight
// surface as context.Canceled / DeadlineExceeded; Session wraps this with
// wait-for-reconnect retry semantics in its own helpers.
func (c *Client) sendRequest(ctx context.Context, command CommandID, data []byte) ([]byte, error) {
	if c.tx.disconnected.Load() {
		return nil, ErrTransportClosed
	}
	c.tx.activeRequestLock.Lock()
	id := c.tx.currentRequest.Add(1)
	responseCh := make(chan []byte, 1)
	c.tx.activeRequests[id] = responseCh
	c.tx.activeRequestLock.Unlock()
	defer func() {
		c.tx.activeRequestLock.Lock()
		delete(c.tx.activeRequests, id)
		c.tx.activeRequestLock.Unlock()
	}()
	c.logger.Log(context.Background(), LevelTrace, "encoding packet",
		"command", command, "data", data, "id", id)

	pack, err := c.encode(command, data, id)
	if err != nil {
		c.logger.Error("Error during sendRequest encode", "error", err)
		return nil, err
	}
	c.tx.chanMu.RLock()
	sendCh := c.tx.sendChannel
	c.tx.chanMu.RUnlock()
	dropped := c.dropped
	// Merge caller ctx with c.requestTimeout. ctx==nil falls back to the
	// Client's own ctx so callers passing context.Background() still get
	// the configured request timeout AND respect Close-driven cancel.
	if ctx == nil {
		ctx = c.ctx
	}
	ctx, cancel := context.WithTimeout(ctx, c.requestTimeout)
	defer cancel()
	select {
	case <-ctx.Done():
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			c.logger.Log(ctx, c.transportFaultLevel(), "sendRequest aborted due to timeout")
		} else {
			c.logger.Info("sendRequest aborted due to shutdown")
		}
		return nil, ctx.Err()
	case <-dropped:
		return nil, ErrTransportClosed
	case sendCh <- pack:
	}
	select {
	case <-ctx.Done():
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			c.logger.Log(ctx, c.transportFaultLevel(), "sendRequest aborted due to timeout")
		} else {
			c.logger.Info("sendRequest aborted due to shutdown")
		}
		return nil, ctx.Err()
	case <-dropped:
		// The transport died while this request was in flight. Returning now
		// instead of waiting out the timeout is what stops a multi-request
		// operation from paying requestTimeout per remaining item.
		return nil, ErrTransportClosed
	case response := <-responseCh:
		return response, nil
	}
}

func (c *Client) dispatchNotification(ctx context.Context, handle uint32, ts uint64, content []byte) {
	c.notifyMu.RLock()
	fn := c.notify
	c.notifyMu.RUnlock()
	if fn == nil {
		c.logger.Debug("DeviceNotification dropped (no handler installed)", "handle", handle)
		return
	}
	fn(ctx, handle, ts, content)
}
