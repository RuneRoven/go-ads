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

	// ondrop is invoked once on listen / transmitWorker error. nil means
	// raw Client (no auto-recovery — caller observes via ErrTransportClosed
	// from subsequent RPCs). Session installs s.triggerReconnect.
	ondropMu sync.RWMutex
	ondrop   func()

	// Internal cancellation for the worker goroutines. Independent of any
	// caller context — Close cancels this to stop workers.
	ctx       context.Context
	cancel    context.CancelFunc
	waitGroup sync.WaitGroup

	closeOnce sync.Once
}

// Dial opens one TCP connection to ip:port, configures TCP keepalive, and
// spawns the listen / transmit / recvWorker goroutines. Returns a usable
// Client. See specs/09-fsm-design.md "Layer 2: Client (raw RPC)".
//
// Phase 5.a-dial: workers run on *Client. RPC methods (Read/Write/etc) and
// sendRequest still live on *Session in this commit; they migrate one
// cluster at a time during Phase 5.b. A raw Client constructed here can
// receive notifications via WithNotificationHandler but cannot yet send
// RPCs until 5.b.1 completes.
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
	c.startWorkers()
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

// SetOnDrop installs a callback fired once when listen or transmitWorker
// observes a transport-level failure. Used by Session to invoke
// triggerReconnect; raw Client consumers leave nil.
func (c *Client) SetOnDrop(fn func()) {
	c.ondropMu.Lock()
	c.ondrop = fn
	c.ondropMu.Unlock()
}

func (c *Client) callOnDrop() {
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
			if isRouteHintErr(err) {
				hint = "PLC may not have an AMS route for this NetID — check route credentials or register route via WithRoute()"
			}
			if hint != "" {
				c.logger.Error("PLC closed connection, transport down",
					"error", err, "hint", hint,
					"sourceNetID", fmt.Sprintf("%d.%d.%d.%d.%d.%d",
						c.source.NetID[0], c.source.NetID[1], c.source.NetID[2],
						c.source.NetID[3], c.source.NetID[4], c.source.NetID[5]))
			} else {
				c.logger.Error("listen read error, transport down", "error", err)
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
			c.logger.Error("listen body read error, transport down", "error", err)
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
	header := amsHeader{}
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
				c.logger.Error("error sending data on conn, transport down", "error", err)
				c.callOnDrop()
				return
			}
			if err := writer.Flush(); err != nil {
				c.logger.Error("error flushing data on conn, transport down", "error", err)
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

// send is the local-mode handshake primitive. NOT safe for concurrent use —
// it consumes from the shared systemResponse channel. Used by Session for
// the local AMS handshake during Connect/Reconnect.
func (c *Client) send(data []byte) ([]byte, error) {
	c.tx.currentRequest.Inc()
	c.tx.chanMu.RLock()
	sendCh := c.tx.sendChannel
	sysCh := c.tx.systemResponse
	c.tx.chanMu.RUnlock()
	ctx, cancel := context.WithTimeout(c.ctx, c.requestTimeout)
	defer cancel()
	select {
	case <-ctx.Done():
		return nil, fmt.Errorf("send aborted, context canceled: %w", ctx.Err())
	case sendCh <- data:
	}
	select {
	case <-ctx.Done():
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			err := fmt.Errorf("request aborted, deadline exceeded: %w", ctx.Err())
			c.logger.Error("send aborted due to timeout", "error", err)
			return nil, err
		}
		err := fmt.Errorf("request aborted, shutdown initiated: %w", ctx.Err())
		c.logger.Error("send aborted due to shutdown", "error", err)
		return nil, err
	case response := <-sysCh:
		return response, nil
	}
}

// sendRequest is the single-shot RPC primitive used by every Client RPC
// method. Encodes the AMS frame, registers a per-invoke response channel,
// pushes the frame to the transmit worker, and waits for the response or
// context timeout.
//
// No retry on transport drop — at the Client layer, drops are terminal
// (caller observes via context.Canceled when c.ctx fires; future calls hit
// the closed sendChannel or block on context). Session wraps this with
// wait-for-reconnect retry semantics in its own sendRequest method.
func (c *Client) sendRequest(command CommandID, data []byte) ([]byte, error) {
	c.tx.activeRequestLock.Lock()
	id := c.tx.currentRequest.Inc()
	c.tx.activeRequests[id] = make(chan []byte, 1)
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
	ctx, cancel := context.WithTimeout(c.ctx, c.requestTimeout)
	defer cancel()
	select {
	case <-ctx.Done():
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			c.logger.Error("sendRequest aborted due to timeout")
		} else {
			c.logger.Info("sendRequest aborted due to shutdown")
		}
		return nil, ctx.Err()
	case sendCh <- pack:
	}
	c.tx.activeRequestLock.Lock()
	responseCh := c.tx.activeRequests[id]
	c.tx.activeRequestLock.Unlock()
	select {
	case <-ctx.Done():
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			c.logger.Error("sendRequest aborted due to timeout")
		} else {
			c.logger.Info("sendRequest aborted due to shutdown")
		}
		return nil, ctx.Err()
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
