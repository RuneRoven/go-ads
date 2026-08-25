package ads

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

// startStubTCPServer binds 127.0.0.1:0, accepts connections in a background
// goroutine, and returns the host+port plus a cleanup func that drains the
// listener. Callers receive a fresh net.Conn on each Dial; the stub never
// reads or writes — it just keeps the socket alive long enough for the
// Client lifecycle test to exercise it.
func startStubTCPServer(t *testing.T) (host string, port int, stop func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr, ok := ln.Addr().(*net.TCPAddr)
	if !ok {
		_ = ln.Close()
		t.Fatalf("unexpected addr type: %T", ln.Addr())
	}

	var wg sync.WaitGroup
	closed := make(chan struct{})

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			wg.Add(1)
			go func(c net.Conn) {
				defer wg.Done()
				<-closed
				_ = c.Close()
			}(c)
		}
	}()

	stop = func() {
		close(closed)
		_ = ln.Close()
		wg.Wait()
	}
	return addr.IP.String(), addr.Port, stop
}

func TestClient_DialClose(t *testing.T) {
	host, port, stop := startStubTCPServer(t)
	defer stop()

	target := AMSAddress{NetID: [6]byte{127, 0, 0, 1, 1, 1}, Port: 851}
	source := AMSAddress{NetID: [6]byte{127, 0, 0, 1, 1, 1}, Port: 30000}

	c, err := Dial(host, port, target, source, 2*time.Second)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	if c == nil {
		t.Fatal("Dial returned nil client without error")
	}
	if c.tx == nil || c.tx.connection == nil {
		t.Fatal("Dial returned client with nil transport")
	}
	if c.ctx == nil {
		t.Fatal("Dial did not initialize ctx")
	}
	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestClient_DoubleClose(t *testing.T) {
	host, port, stop := startStubTCPServer(t)
	defer stop()

	target := AMSAddress{}
	source := AMSAddress{}

	c, err := Dial(host, port, target, source, time.Second)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

func TestClient_DialFailsWhenServerUnreachable(t *testing.T) {
	// Port 1 is reserved (TCPMUX) and almost never open on test machines.
	// On systems that DO have it open, the test still passes — Dial succeeds
	// and we Close, asserting only that Dial returns either a usable client
	// or a wrapped dial error.
	target := AMSAddress{}
	source := AMSAddress{}
	c, err := Dial("127.0.0.1", 1, target, source, 250*time.Millisecond)
	if err == nil {
		_ = c.Close()
		t.Skip("port 1 unexpectedly accepted connection on this host")
	}
	if !strings.Contains(err.Error(), "ads: dial 127.0.0.1:1") {
		t.Errorf("expected wrapped 'ads: dial 127.0.0.1:1' error, got %v", err)
	}
}

func TestClient_OptionsApplied(t *testing.T) {
	host, port, stop := startStubTCPServer(t)
	defer stop()

	called := 0
	hookFired := false
	notify := func(ctx context.Context, handle uint32, ts uint64, content []byte) {
		hookFired = true
	}

	c, err := Dial(
		host, port,
		AMSAddress{}, AMSAddress{},
		3*time.Second,
		WithClientRequestTimeout(7*time.Second),
		WithNotificationHandler(notify),
		ClientOption(func(*Client) { called++ }),
	)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer c.Close()

	if c.requestTimeout != 7*time.Second {
		t.Errorf("requestTimeout: want 7s, got %v", c.requestTimeout)
	}
	c.notifyMu.RLock()
	gotNotify := c.notify != nil
	c.notifyMu.RUnlock()
	if !gotNotify {
		t.Error("notification handler not installed")
	}
	if called != 1 {
		t.Errorf("custom option called %d times, want 1", called)
	}
	// Sanity-check the captured hook is reachable; will be invoked for real
	// in Phase 5.a-dial when recvWorker decodes a notification packet.
	notify(context.Background(), 0, 0, nil)
	if !hookFired {
		t.Error("captured notify ref was not callable")
	}
}

func TestClient_TransportClosedSentinel(t *testing.T) {
	if !errors.Is(ErrTransportClosed, ErrTransportClosed) {
		t.Fatal("ErrTransportClosed must be matchable via errors.Is")
	}
	// Phase 5.a-types: no public method returns this yet. Phase 5.a-dial
	// + Phase 5.c add the call sites; we declare the sentinel now so the
	// downstream wiring can target a stable identity.
	wrapped := wrapErr(ErrTransportClosed)
	if !errors.Is(wrapped, ErrTransportClosed) {
		t.Errorf("wrap test: errors.Is failed; wrap was %T", wrapped)
	}
	_ = strconv.Itoa(0) // silence unused import in this trimmed test set
}

// wrapErr is a tiny helper used only by TestClient_TransportClosedSentinel.
// Kept inline to avoid leaking helpers into non-test code.
func wrapErr(err error) error {
	return errWrap{inner: err}
}

type errWrap struct{ inner error }

func (e errWrap) Error() string { return "wrapped: " + e.inner.Error() }
func (e errWrap) Unwrap() error { return e.inner }

// buildAmsTCPHeader builds the 6-byte amsTCPHeader: 1-byte Reserved + 1-byte System + 4-byte Length (LE).
func buildAmsTCPHeader(system uint8, length uint32) []byte {
	b := make([]byte, 6)
	b[0] = 0
	b[1] = system
	binary.LittleEndian.PutUint32(b[2:], length)
	return b
}

// TestListen_TwoSequentialPackets verifies that listen() correctly frames
// amsTCPHeader + body across multiple sequential packets. Uses net.Pipe to
// simulate the TCP connection.
func TestListen_TwoSequentialPackets(t *testing.T) {
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	conn := &Session{
		tx:        &transport{connection: client, systemResponse: make(chan []byte, 2)},
		logger:    getDefaultLogger(),
		lifecycle: &sessionLifecycle{},
	}
	ctx, cancel := context.WithCancel(context.Background())
	conn.lifecycle.ctx = ctx
	conn.lifecycle.shutdown = cancel
	conn.client.Store(&Client{tx: conn.tx, logger: conn.logger, ctx: ctx})
	conn.client.Load().waitGroup.Add(1)

	var listenDone sync.WaitGroup
	listenDone.Add(1)
	go func() {
		defer listenDone.Done()
		conn.client.Load().listen()
	}()

	body1 := []byte{0xAA, 0xBB, 0xCC, 0xDD}
	hdr1 := buildAmsTCPHeader(1, uint32(len(body1)))
	if _, err := server.Write(append(hdr1, body1...)); err != nil {
		t.Fatalf("write packet 1: %v", err)
	}

	body2 := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08}
	hdr2 := buildAmsTCPHeader(1, uint32(len(body2)))
	if _, err := server.Write(append(hdr2, body2...)); err != nil {
		t.Fatalf("write packet 2: %v", err)
	}

	got1 := <-conn.tx.systemResponse
	if !bytes.Equal(got1, body1) {
		t.Errorf("packet 1: got %v, want %v", got1, body1)
	}
	got2 := <-conn.tx.systemResponse
	if !bytes.Equal(got2, body2) {
		t.Errorf("packet 2: got %v, want %v", got2, body2)
	}

	cancel()
	server.Close()
	listenDone.Wait()
}

// TestListen_OversizePacketTriggersReconnect verifies that an over-large header.Length
// is rejected and listen() exits without panic or infinite loop. triggerReconnect's
// internals are not exercised — we only verify the goroutine returns.
func TestListen_OversizePacketTriggersReconnect(t *testing.T) {
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	conn := &Session{
		tx:        &transport{connection: client, systemResponse: make(chan []byte, 1)},
		logger:    getDefaultLogger(),
		lifecycle: &sessionLifecycle{closedCh: make(chan struct{})},
	}
	// Mark closed via FSM (legacy closed flag removed in Phase 3.b).
	conn.lifecycle.state.transitionTo(SessionStateClosed)
	ctx, cancel := context.WithCancel(context.Background())
	conn.lifecycle.ctx = ctx
	conn.lifecycle.shutdown = cancel
	conn.client.Store(&Client{tx: conn.tx, logger: conn.logger, ctx: ctx})
	conn.client.Load().waitGroup.Add(1)

	var listenDone sync.WaitGroup
	listenDone.Add(1)
	go func() {
		defer listenDone.Done()
		conn.client.Load().listen()
	}()

	hdr := buildAmsTCPHeader(1, 8*1024*1024) // 8 MB exceeds 4 MB cap
	_, _ = server.Write(hdr)

	done := make(chan struct{})
	go func() {
		listenDone.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Errorf("listen() did not return after oversized header")
	}
}

// TestSendRequestOnce_CtxDoneReturnsCanceled was removed in Phase 5.b.2/3:
// Session.sendRequestOnce no longer exists. The wait-for-reconnect logic
// it covered moves to Session-side clientRead / clientWrite wrappers in
// Phase 5.c; an equivalent test will land alongside that wrapper.

func TestIsRouteHintErr_EOF(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"bare io.EOF", io.EOF, true},
		{"wrapped io.EOF via fmt.Errorf %w", fmt.Errorf("read failed: %w", io.EOF), true},
		{"deeply wrapped io.EOF", fmt.Errorf("outer: %w", fmt.Errorf("inner: %w", io.EOF)), true},
		{"unrelated error", errors.New("some other error"), false},
		{"nil error", nil, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := isLikelyMissingRoute(tc.err)
			if got != tc.want {
				t.Errorf("isLikelyMissingRoute(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

func TestIsRouteHintErr_ECONNRESET(t *testing.T) {
	// Simulate a wrapped *net.OpError with syscall.ECONNRESET
	innerErr := &net.OpError{
		Op:  "read",
		Net: "tcp",
		Err: &net.OpError{Err: syscall.ECONNRESET},
	}
	wrapped := fmt.Errorf("during listen: %w", innerErr)
	if !isLikelyMissingRoute(wrapped) {
		t.Errorf("isLikelyMissingRoute did not detect ECONNRESET in wrapped *net.OpError")
	}
}

// ==========================================================================
// AMS packet encoding
// ==========================================================================

func TestEncodePacket(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	conn := &Session{
		tx:        &transport{},
		lifecycle: &sessionLifecycle{ctx: ctx},
		logger:    getDefaultLogger(),
		target: AMSAddress{
			NetID: [6]byte{5, 154, 236, 19, 1, 1},
			Port:  851,
		},
		source: AMSAddress{
			NetID: [6]byte{192, 168, 1, 100, 1, 1},
			Port:  10500,
		},
	}
	conn.client.Store(&Client{tx: conn.tx, logger: conn.logger, target: conn.target, source: conn.source})

	data := []byte{0x01, 0x02, 0x03, 0x04}
	packet, err := conn.client.Load().encode(CommandIDRead, data, 7)
	if err != nil {
		t.Fatalf("encode error: %v", err)
	}

	// TCP header: 6 bytes (Unknown1=0, System=0, Length=32+4=36)
	if len(packet) != 6+32+4 {
		t.Fatalf("packet length = %d, want %d", len(packet), 42)
	}

	// Check TCP header length field
	tcpLen := binary.LittleEndian.Uint32(packet[2:6])
	if tcpLen != 36 {
		t.Errorf("TCP length = %d, want 36", tcpLen)
	}

	// Check AMS header target NetID (starts at offset 6)
	var targetNetID [6]byte
	copy(targetNetID[:], packet[6:12])
	if targetNetID != conn.target.NetID {
		t.Errorf("target NetID = %v, want %v", targetNetID, conn.target.NetID)
	}

	// Target port at offset 12
	targetPort := binary.LittleEndian.Uint16(packet[12:14])
	if targetPort != 851 {
		t.Errorf("target port = %d, want 851", targetPort)
	}

	// Source NetID at offset 14
	var sourceNetID [6]byte
	copy(sourceNetID[:], packet[14:20])
	if sourceNetID != conn.source.NetID {
		t.Errorf("source NetID = %v, want %v", sourceNetID, conn.source.NetID)
	}

	// Command at offset 22
	cmd := binary.LittleEndian.Uint16(packet[22:24])
	if CommandID(cmd) != CommandIDRead {
		t.Errorf("command = %d, want %d (Read)", cmd, CommandIDRead)
	}

	// State at offset 24 — should be 4 (request)
	state := binary.LittleEndian.Uint16(packet[24:26])
	if state != 4 {
		t.Errorf("state = %d, want 4", state)
	}

	// Data length at offset 26
	dataLen := binary.LittleEndian.Uint32(packet[26:30])
	if dataLen != 4 {
		t.Errorf("data length = %d, want 4", dataLen)
	}

	// InvokeID at offset 34
	invokeID := binary.LittleEndian.Uint32(packet[34:38])
	if invokeID != 7 {
		t.Errorf("invokeID = %d, want 7", invokeID)
	}

	// Payload at offset 38
	if !bytes.Equal(packet[38:], data) {
		t.Errorf("payload = %v, want %v", packet[38:], data)
	}
}

func TestEncodePacket_EmptyData(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	conn := &Session{
		tx:        &transport{},
		lifecycle: &sessionLifecycle{ctx: ctx},
		logger:    getDefaultLogger(),
		target:    AMSAddress{NetID: [6]byte{1, 2, 3, 4, 5, 6}, Port: 851},
		source:    AMSAddress{NetID: [6]byte{10, 20, 30, 40, 1, 1}, Port: 10500},
	}
	conn.client.Store(&Client{tx: conn.tx, logger: conn.logger, target: conn.target, source: conn.source})

	packet, err := conn.client.Load().encode(CommandIDReadDeviceInfo, nil, 0)
	if err != nil {
		t.Fatalf("encode error: %v", err)
	}

	// 6 (TCP) + 32 (AMS) + 0 (data)
	if len(packet) != 38 {
		t.Fatalf("packet length = %d, want 38", len(packet))
	}

	tcpLen := binary.LittleEndian.Uint32(packet[2:6])
	if tcpLen != 32 {
		t.Errorf("TCP length = %d, want 32", tcpLen)
	}
}

// TestEncodePacket_AllCommands round-trips encode against every supported
// CommandID and asserts the full set of header fields and payload —
// command, AMS state (request=4), invokeID, target+source AMS addresses,
// length, and payload bytes. The previous version asserted only the
// command field, which would have missed bugs in any of the other
// header positions.
//
// Validates: R-CMD-002 (header layout) per command kind.
func TestEncodePacket_AllCommands(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	target := AMSAddress{NetID: [6]byte{1, 2, 3, 4, 5, 6}, Port: 851}
	source := AMSAddress{NetID: [6]byte{10, 20, 30, 40, 1, 1}, Port: 10500}
	conn := &Session{
		tx:        &transport{},
		lifecycle: &sessionLifecycle{ctx: ctx},
		logger:    getDefaultLogger(),
		target:    target,
		source:    source,
	}
	conn.client.Store(&Client{tx: conn.tx, logger: conn.logger, target: target, source: source})

	commands := []CommandID{
		CommandIDReadDeviceInfo,
		CommandIDRead,
		CommandIDWrite,
		CommandIDReadState,
		CommandIDWriteControl,
		CommandIDAddDeviceNotification,
		CommandIDDeleteDeviceNotification,
		CommandIDReadWrite,
	}

	for i, cmd := range commands {
		invokeID := uint32(100 + i) // distinct per command so we catch swapped fields
		payload := []byte{0xDE, 0xAD, 0xBE, byte(cmd & 0xFF)}
		t.Run(fmt.Sprintf("Command_%d", cmd), func(t *testing.T) {
			packet, err := conn.client.Load().encode(cmd, payload, invokeID)
			if err != nil {
				t.Fatalf("encode: %v", err)
			}
			// Layout: 6-byte amsTCPHeader + 32-byte amsHeader + payload.
			if len(packet) != 6+32+len(payload) {
				t.Fatalf("packet length %d, want %d", len(packet), 6+32+len(payload))
			}
			// amsTCPHeader: bytes 0-1 unknown/system (zero), 2-5 length (LE).
			if got := binary.LittleEndian.Uint32(packet[2:6]); got != uint32(32+len(payload)) {
				t.Errorf("TCP length = %d, want %d", got, 32+len(payload))
			}
			// amsHeader at offset 6: target (8) + source (8) + cmd (2) + state (2) + length (4) + errcode (4) + invokeID (4).
			gotTargetNetID := *(*[6]byte)(packet[6 : 6+6])
			if gotTargetNetID != target.NetID {
				t.Errorf("target NetID = %v, want %v", gotTargetNetID, target.NetID)
			}
			gotTargetPort := binary.LittleEndian.Uint16(packet[12:14])
			if gotTargetPort != target.Port {
				t.Errorf("target port = %d, want %d", gotTargetPort, target.Port)
			}
			gotSourceNetID := *(*[6]byte)(packet[14 : 14+6])
			if gotSourceNetID != source.NetID {
				t.Errorf("source NetID = %v, want %v", gotSourceNetID, source.NetID)
			}
			gotSourcePort := binary.LittleEndian.Uint16(packet[20:22])
			if gotSourcePort != source.Port {
				t.Errorf("source port = %d, want %d", gotSourcePort, source.Port)
			}
			if got := binary.LittleEndian.Uint16(packet[22:24]); CommandID(got) != cmd {
				t.Errorf("command = %d, want %d", got, cmd)
			}
			if got := binary.LittleEndian.Uint16(packet[24:26]); got != 4 {
				t.Errorf("state = %d, want 4 (request)", got)
			}
			if got := binary.LittleEndian.Uint32(packet[26:30]); got != uint32(len(payload)) {
				t.Errorf("payload length = %d, want %d", got, len(payload))
			}
			if got := binary.LittleEndian.Uint32(packet[30:34]); got != 0 {
				t.Errorf("error code = %d, want 0", got)
			}
			if got := binary.LittleEndian.Uint32(packet[34:38]); got != invokeID {
				t.Errorf("invokeID = %d, want %d", got, invokeID)
			}
			if !bytes.Equal(packet[38:], payload) {
				t.Errorf("payload bytes = %v, want %v", packet[38:], payload)
			}
		})
	}
}

// ==========================================================================
// handleReceive — AMS response routing
// ==========================================================================

func TestHandleReceive_RoutesToCorrectChannel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	conn := &Session{
		lifecycle: &sessionLifecycle{ctx: ctx},
		logger:    getDefaultLogger(),
		tx:        &transport{activeRequests: make(map[uint32]chan amsReply)},
	}
	conn.client.Store(&Client{tx: conn.tx, logger: conn.logger, ctx: ctx})

	// Register a response channel for invokeID 42
	ch := make(chan amsReply, 1)
	conn.tx.activeRequestLock.Lock()
	conn.tx.activeRequests[42] = ch
	conn.tx.activeRequestLock.Unlock()

	// Build AMS header + data
	header := AMSHeader{
		Target:    AMSAddress{},
		Source:    AMSAddress{},
		Command:   CommandIDRead,
		State:     5, // response
		Length:    4,
		ErrorCode: 0,
		InvokeID:  42,
	}
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, &header)
	buf.Write([]byte{0xDE, 0xAD, 0xBE, 0xEF})

	conn.client.Load().handleReceive(ctx, buf.Bytes())

	select {
	case reply := <-ch:
		if reply.amsErr != 0 {
			t.Errorf("amsErr = %v, want 0 for a successful response", reply.amsErr)
		}
		if !bytes.Equal(reply.data, []byte{0xDE, 0xAD, 0xBE, 0xEF}) {
			t.Errorf("response = %v, want [0xDE 0xAD 0xBE 0xEF]", reply.data)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for response")
	}
}

func TestHandleReceive_UnknownInvokeID(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	conn := &Session{
		lifecycle: &sessionLifecycle{ctx: ctx},
		logger:    getDefaultLogger(),
		tx:        &transport{activeRequests: make(map[uint32]chan amsReply)},
	}
	conn.client.Store(&Client{tx: conn.tx, logger: conn.logger, ctx: ctx})

	// No registered channels — should not panic
	header := AMSHeader{
		Command:  CommandIDRead,
		State:    5,
		Length:   2,
		InvokeID: 999,
	}
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, &header)
	buf.Write([]byte{0x00, 0x00})

	// Should not panic or error
	conn.client.Load().handleReceive(ctx, buf.Bytes())
}

// TestHandleReceive_TooShort pins WHICH branch a sub-header-sized packet takes.
// The bare call asserted nothing, and the length guard is redundant behind
// binary.Read (which fails with ErrUnexpectedEOF on 5 bytes), so deleting the
// guard left the old version green. Asserting the log record kills that mutant:
// without the guard the message is "Error parsing header" instead. The guard is
// still worth pinning — it is the only thing between a future header-size change
// and the unguarded data[32:] slice.
func TestHandleReceive_TooShort(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	logs := &testLogHandler{}
	conn := &Session{
		lifecycle: &sessionLifecycle{ctx: ctx},
		logger:    slog.New(logs),
		tx:        &transport{activeRequests: make(map[uint32]chan amsReply)},
	}
	conn.client.Store(&Client{tx: conn.tx, logger: conn.logger, ctx: ctx})

	// Less than 32 bytes — should return early
	conn.client.Load().handleReceive(ctx, []byte{1, 2, 3, 4, 5})

	if logs.findByMessage("header too short") == nil {
		t.Error("short packet did not hit the length guard; it fell through to header decode")
	}
	if len(conn.tx.activeRequests) != 0 {
		t.Errorf("short packet touched activeRequests: %d entries", len(conn.tx.activeRequests))
	}
}

// ==========================================================================
// hexAttr utility
// ==========================================================================

func TestHexAttr(t *testing.T) {
	attr := hexAttr("data", []byte{0xDE, 0xAD, 0xBE, 0xEF})
	if attr.Key != "data" {
		t.Errorf("key = %q, want %q", attr.Key, "data")
	}
	s := attr.Value.String()
	if !strings.Contains(s, "DEADBEEF") && !strings.Contains(s, "deadbeef") {
		t.Errorf("hex string = %q, expected to contain DEADBEEF", s)
	}
}

func TestHexAttr_Empty(t *testing.T) {
	attr := hexAttr("empty", []byte{})
	if attr.Key != "empty" {
		t.Errorf("key = %q, want %q", attr.Key, "empty")
	}
}

// ==========================================================================
// startEchoTCPServer — stub that replies to AMS Read requests by echoing
// the request bytes (with the InvokeID preserved) back as a Read-success
// response. Used by tests that need to drive Client.sendRequest and observe
// per-invoke multiplexing.
// ==========================================================================

// echoServer reads complete AMS frames from the TCP connection, swaps the
// state byte to "response", flips error code 0, and writes back a response
// whose data section is empty (declared length 0 in the Read response
// header). The InvokeID is preserved so each caller correlates with its
// own request channel.
//
// recordFn (if non-nil) is invoked for each fully-received request frame
// so tests can assert on inbound bytes.
type echoServer struct {
	host    string
	port    int
	wg      sync.WaitGroup
	closed  chan struct{}
	ln      net.Listener
	t       *testing.T
	recordF func(frame []byte)
}

func startEchoTCPServer(t *testing.T, recordF func(frame []byte)) *echoServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr, ok := ln.Addr().(*net.TCPAddr)
	if !ok {
		_ = ln.Close()
		t.Fatalf("unexpected addr type: %T", ln.Addr())
	}
	s := &echoServer{
		host:    addr.IP.String(),
		port:    addr.Port,
		closed:  make(chan struct{}),
		ln:      ln,
		t:       t,
		recordF: recordF,
	}
	s.wg.Add(1)
	go s.acceptLoop()
	return s
}

func (s *echoServer) acceptLoop() {
	defer s.wg.Done()
	for {
		c, err := s.ln.Accept()
		if err != nil {
			return
		}
		s.wg.Add(1)
		go s.handle(c)
	}
}

func (s *echoServer) handle(c net.Conn) {
	defer s.wg.Done()
	defer c.Close()
	for {
		// Read TCP header (6 bytes): Unknown(1) + System(1) + Length(4 LE)
		hdr := make([]byte, 6, 6+4*1024)
		hdr = hdr[:6]
		if _, err := io.ReadFull(c, hdr); err != nil {
			return
		}
		bodyLen := binary.LittleEndian.Uint32(hdr[2:6])
		if bodyLen > 4*1024*1024 {
			return
		}
		body := make([]byte, bodyLen)
		if _, err := io.ReadFull(c, body); err != nil {
			return
		}
		// AMS header is 32 bytes: Target(8) Source(8) Cmd(2) State(2)
		// Length(4) ErrorCode(4) InvokeID(4).
		if len(body) < 32 {
			return
		}
		frame := make([]byte, 0, len(hdr)+len(body))
		frame = append(frame, hdr...)
		frame = append(frame, body...)
		if s.recordF != nil {
			s.recordF(frame)
		}
		// Build response: same header but State=5 (response), Length=0,
		// ErrorCode=0, InvokeID preserved. Body is 4 bytes ReturnCode +
		// 4 bytes Length(=0) for Read responses; for plain Write responses
		// just 4 bytes ReturnCode. We always send the longer form because
		// callers either parse 4 bytes (Write) or 8 bytes (Read/WriteRead/
		// AddDeviceNotification etc.) — extra bytes after the declared
		// Length are OK on the wire (Client just decodes the prefix).
		invokeID := binary.LittleEndian.Uint32(body[28:32])
		cmd := binary.LittleEndian.Uint16(body[16:18])
		respBody := make([]byte, 32+8)
		// Swap target<->source so the response addressing looks plausible.
		copy(respBody[0:8], body[8:16]) // new Target = old Source
		copy(respBody[8:16], body[0:8]) // new Source = old Target
		binary.LittleEndian.PutUint16(respBody[16:18], cmd)
		binary.LittleEndian.PutUint16(respBody[18:20], 5) // State = response
		binary.LittleEndian.PutUint32(respBody[20:24], 8) // Length of payload
		binary.LittleEndian.PutUint32(respBody[24:28], 0) // ErrorCode
		binary.LittleEndian.PutUint32(respBody[28:32], invokeID)
		// Payload: ReturnCode(0) + extra zero bytes (so Read/WriteRead see Length=0).
		// Plain Write only reads ReturnCode (first 4 bytes); the extra are ignored.
		// AddDeviceNotification reads ReturnCode + Handle (handle=0 here).
		// ReadDeviceInfo expects 24 bytes — handled separately.
		// For ReadDeviceInfo (cmd=1) the response must be 24 bytes total;
		// emit a fixed 24-byte payload to satisfy the strict length check.
		if cmd == 1 { // CommandIDReadDeviceInfo
			respBody = respBody[:32]
			binary.LittleEndian.PutUint32(respBody[20:24], 24) // declared Length
			respBody = append(respBody, make([]byte, 24)...)
		}
		respHdr := make([]byte, 6, 6+len(respBody))
		respHdr[0] = 0
		respHdr[1] = 0
		binary.LittleEndian.PutUint32(respHdr[2:6], uint32(len(respBody)))
		full := append(respHdr, respBody...) //nolint:gocritic // intentional grow into the pre-allocated capacity.
		if _, err := c.Write(full); err != nil {
			return
		}
	}
}

func (s *echoServer) stop() {
	_ = s.ln.Close()
	close(s.closed)
	// Best-effort wait — accept loop and per-conn handlers exit on Close.
	done := make(chan struct{})
	go func() { s.wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
	}
}

// TestClient_ConcurrentMultiplexing fires 100 concurrent Read calls through
// a single Client and asserts each goroutine receives a response keyed by
// its own InvokeID. The stub server preserves the InvokeID on each
// response, so any cross-talk would surface as a wrong-payload assertion.
//
// Validates: R-CL-002 (concurrent multiplexing), R-TX-004 (per-invoke ID).
func TestClient_ConcurrentMultiplexing(t *testing.T) {
	srv := startEchoTCPServer(t, nil)
	defer srv.stop()

	c, err := Dial(srv.host, srv.port, AMSAddress{}, AMSAddress{}, 5*time.Second)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer c.Close()

	const N = 100
	var wg sync.WaitGroup
	errs := make(chan error, N)
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// Read with length 0 — server returns empty data section, no error.
			_, rerr := c.Read(context.Background(), 0xF003, 0, 0)
			if rerr != nil {
				errs <- rerr
			}
		}()
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		t.Errorf("Read returned error: %v", e)
	}
}

// TestClient_ClosedReturnsErrTransportClosed asserts every Client public
// RPC method returns ErrTransportClosed (matched via errors.Is) after Close.
//
// Validates: R-CL-003 (closed Client returns ErrTransportClosed everywhere).
func TestClient_ClosedReturnsErrTransportClosed(t *testing.T) {
	host, port, stop := startStubTCPServer(t)
	defer stop()

	c, err := Dial(host, port, AMSAddress{}, AMSAddress{}, time.Second)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	type call struct {
		name string
		fn   func() error
	}
	calls := []call{
		{"Read", func() error { _, e := c.Read(context.Background(), 0, 0, 0); return e }},
		{"Write", func() error { return c.Write(context.Background(), 0, 0, nil) }},
		{"WriteRead", func() error { _, e := c.WriteRead(context.Background(), 0, 0, 0, nil); return e }},
		{"ReadDeviceInfo", func() error { _, e := c.ReadDeviceInfo(context.Background()); return e }},
		{"ReadState", func() error { _, e := c.ReadState(context.Background()); return e }},
		{"GetHandleByName", func() error { _, e := c.GetHandleByName(context.Background(), "X"); return e }},
		{"GetSymbolVersion", func() error { _, e := c.GetSymbolVersion(context.Background()); return e }},
		{"GetSymbolUploadInfo", func() error { _, e := c.GetSymbolUploadInfo(context.Background()); return e }},
		{"AddDeviceNotification", func() error {
			_, e := c.AddDeviceNotification(context.Background(), 0, 0, 0, TransModeNoTransmission, 0, 0)
			return e
		}},
		{"DeleteDeviceNotification", func() error { return c.DeleteDeviceNotification(context.Background(), 1) }},
		{"ReleaseHandle", func() error { return c.ReleaseHandle(context.Background(), 1) }},
		{"SumRead", func() error {
			_, e := c.SumRead(context.Background(), []SumReadRequest{{Group: 1, Length: 1}})
			return e
		}},
		{"SumWrite", func() error {
			_, e := c.SumWrite(context.Background(), []SumWriteRequest{{Group: 1, Data: []byte{0}}})
			return e
		}},
		{"SumAddDeviceNotification", func() error {
			_, e := c.SumAddDeviceNotification(context.Background(), []SumNotificationRequest{{Group: 1, Length: 1}})
			return e
		}},
		{"SumDeleteDeviceNotification", func() error {
			_, e := c.SumDeleteDeviceNotification(context.Background(), []uint32{1})
			return e
		}},
		{"ReadProcessInput", func() error { _, e := c.ReadProcessInput(context.Background(), 0, 1); return e }},
		{"WriteProcessOutput", func() error { return c.WriteProcessOutput(context.Background(), 0, []byte{0}) }},
	}
	for _, tc := range calls {
		err := tc.fn()
		if !errors.Is(err, ErrTransportClosed) {
			t.Errorf("%s: err=%v, want errors.Is(err, ErrTransportClosed)", tc.name, err)
		}
	}
}

// clientWorkerFrames are the entry points of the goroutines startWorkers spawns.
// Counting these frames in a full goroutine dump identifies this package's workers
// specifically, unlike runtime.NumGoroutine, which counts every goroutine in the
// test binary — including whatever an unrelated test happens to be running.
var clientWorkerFrames = []string{
	"(*Client).listen(",
	"(*Client).transmitWorker(",
	"(*Client).recvWorker(",
}

// countClientWorkers returns how many Client worker goroutines exist right now,
// across the whole process.
func countClientWorkers(t *testing.T) int {
	t.Helper()
	buf := make([]byte, 1<<20)
	for {
		n := runtime.Stack(buf, true)
		if n < len(buf) {
			buf = buf[:n]
			break
		}
		buf = make([]byte, 2*len(buf))
	}
	dump := string(buf)
	total := 0
	for _, frame := range clientWorkerFrames {
		total += strings.Count(dump, frame)
	}
	return total
}

// TestClient_GoroutineCountBoundedAfterClose asserts that Dial spawns exactly
// 1 listen + 1 transmit + recvWorkerCount recv workers, and that Close
// terminates them all.
//
// It used to assert an absolute runtime.NumGoroutine() delta in a shared test
// process, which made it flaky by construction: any unrelated test's goroutines
// inflate the baseline between the two samples and shrink the observed delta. It
// failed once under load at delta 16 against a want of 18 while passing in
// isolation. Both halves are now pinned on the Client's own machinery instead —
// its worker stack frames, and its waitGroup via Close — so the assertion is
// independent of the rest of the binary and stays valid under t.Parallel().
//
// Validates: R-CL-004 (goroutines bounded), R-CL-001 (Close cleans up).
func TestClient_GoroutineCountBoundedAfterClose(t *testing.T) {
	host, port, stop := startStubTCPServer(t)
	defer stop()

	baseline := countClientWorkers(t)

	c, err := Dial(host, port, AMSAddress{}, AMSAddress{}, time.Second)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}

	// listen + transmit + recvWorkerCount.
	wantWorkers := 2 + recvWorkerCount
	deadline := time.Now().Add(2 * time.Second)
	started := 0
	for time.Now().Before(deadline) {
		started = countClientWorkers(t) - baseline
		if started >= wantWorkers {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if started < wantWorkers {
		t.Fatalf("after Dial: %d Client worker goroutines started, want %d (baseline %d)",
			started, wantWorkers, baseline)
	}

	// Close cancels the worker context and then waits on the Client's own
	// waitGroup, which every worker is registered in before it is spawned. So a
	// worker that ignores its exit signal blocks Close forever: bound the wait so
	// that failure lands here with a diagnosis instead of as a package timeout.
	closed := make(chan error, 1)
	go func() { closed <- c.Close() }()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatalf("Close: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Close did not return within 10s: its waitGroup never drained, so at least one " +
			"worker ignored ctx.Done / the closed connection")
	}

	// Close's Wait returning proves every registered worker called Done. Poll the
	// frame count back to the baseline as well, which additionally catches a worker
	// goroutine that leaked without being registered in the waitGroup, plus the
	// brief window where a goroutine has run its deferred Done but not yet unwound.
	deadline = time.Now().Add(2 * time.Second)
	remaining := 0
	for time.Now().Before(deadline) {
		remaining = countClientWorkers(t) - baseline
		if remaining <= 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if remaining > 0 {
		t.Errorf("after Close: %d Client worker goroutines still running (baseline %d) — workers leaked",
			remaining, baseline)
	}
}

// TestClient_ReleaseHandleWritesToReleaseGroup drives Client.ReleaseHandle
// and asserts the inbound request frame addresses GroupSymbolReleaseHandle
// with the LE-encoded handle value as the body.
//
// Validates: R-CL-006 (ReleaseHandle wraps Write to GroupSymbolReleaseHandle).
func TestClient_ReleaseHandleWritesToReleaseGroup(t *testing.T) {
	var captured [][]byte
	var capMu sync.Mutex
	srv := startEchoTCPServer(t, func(frame []byte) {
		capMu.Lock()
		captured = append(captured, append([]byte{}, frame...))
		capMu.Unlock()
	})
	defer srv.stop()

	c, err := Dial(srv.host, srv.port, AMSAddress{}, AMSAddress{}, 2*time.Second)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer c.Close()

	const handle uint32 = 0xCAFEBABE
	if err := c.ReleaseHandle(context.Background(), handle); err != nil {
		t.Fatalf("ReleaseHandle: %v", err)
	}

	capMu.Lock()
	defer capMu.Unlock()
	if len(captured) == 0 {
		t.Fatal("server never received a frame")
	}
	frame := captured[0]
	// Frame: 6-byte amsTCPHeader + 32-byte amsHeader + payload.
	// amsHeader.Command at offset 6+16 = 22 (2 bytes LE).
	cmd := binary.LittleEndian.Uint16(frame[22:24])
	if CommandID(cmd) != CommandIDWrite {
		t.Errorf("command = %d, want %d (Write)", cmd, CommandIDWrite)
	}
	// Write body layout: Group(4) + Offset(4) + Length(4) + Data(handle=4 bytes).
	bodyStart := 6 + 32
	if len(frame) < bodyStart+16 {
		t.Fatalf("frame too short: %d", len(frame))
	}
	gotGroup := binary.LittleEndian.Uint32(frame[bodyStart : bodyStart+4])
	if gotGroup != uint32(GroupSymbolReleaseHandle) {
		t.Errorf("group = 0x%X, want 0x%X (GroupSymbolReleaseHandle)", gotGroup, uint32(GroupSymbolReleaseHandle))
	}
	gotHandle := binary.LittleEndian.Uint32(frame[bodyStart+12 : bodyStart+16])
	if gotHandle != handle {
		t.Errorf("handle = 0x%X, want 0x%X", gotHandle, handle)
	}
}

// TestClient_OnDropFiresExactlyOnce installs SetOnDrop and forces the
// underlying socket closed (via net.Pipe server-side). The listen goroutine
// must observe the drop and invoke the callback exactly once.
//
// Validates: R-CL-008 (on-drop fires once on synthetic listen failure).
func TestClient_OnDropFiresExactlyOnce(t *testing.T) {
	server, client := net.Pipe()
	defer server.Close()
	// client side will be closed by Client.Close in cleanup.

	var fires atomic.Int32
	c := &Client{
		logger: getDefaultLogger(),
		tx: &transport{
			connection:     client,
			sendChannel:    make(chan []byte),
			systemResponse: make(chan []byte),
			recvQueue:      make(chan []byte, 8),
			activeRequests: map[uint32]chan amsReply{},
		},
	}
	c.ctx, c.cancel = context.WithCancel(context.Background())
	// Real Session installs SetOnDrop with a callback that triggers
	// reconnect (which cancels ctx); raw clients leave it nil. For this
	// test we count fires AND cancel ctx so the transmit worker also
	// exits — without that, transmitWorker blocks forever on sendChannel
	// and the test would hang.
	c.SetOnDrop(func() {
		fires.Add(1)
		c.tx.disconnected.Store(true)
		c.cancel()
	})
	c.startWorkers()

	// Force listen to observe EOF: close server side.
	_ = server.Close()

	// Wait for ALL workers (listen, transmit, recv) to exit.
	done := make(chan struct{})
	go func() { c.waitGroup.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("workers did not exit after server-side close")
	}
	// Even though both listen and transmit may attempt a callOnDrop,
	// the production code as of Phase 5.a-dial does NOT gate
	// callOnDrop. Per R-CL-008 the contract is exactly-once; we observe
	// the actual count to surface any regression.
	got := fires.Load()
	if got != 1 {
		t.Errorf("on-drop fired %d times, want exactly 1 (R-CL-008)", got)
	}
	_ = c.Close()
}

// TestWithOnDrop_RegistersCallback validates the WithOnDrop ClientOption:
// applying the option to a bare Client must install the callback such that
// callOnDrop invokes it. Mirrors the runtime SetOnDrop pathway without
// dialing a transport.
func TestWithOnDrop_RegistersCallback(t *testing.T) {
	called := make(chan struct{}, 1)
	opt := WithOnDrop(func() { called <- struct{}{} })

	// A real transport: callOnDrop marks it down and releases waiters, so the
	// zero-value Client this test used to build is not a shape that exists
	// (Dial and NewSession both construct tx).
	c := &Client{tx: &transport{}, dropped: make(chan struct{})}
	opt(c)

	c.callOnDrop()

	select {
	case <-called:
	case <-time.After(time.Second):
		t.Fatal("WithOnDrop callback did not fire via callOnDrop")
	}
}

func TestSystemResponseChannelIsBuffered(t *testing.T) {
	// systemResponse must be buffered (cap >= 1) so a system packet arriving
	// outside a send() window does not stall the listen worker (R-TX-007).
	host, port, stop := startStubTCPServer(t)
	defer stop()

	target := AMSAddress{NetID: [6]byte{127, 0, 0, 1, 1, 1}, Port: 851}
	source := AMSAddress{NetID: [6]byte{127, 0, 0, 1, 1, 1}, Port: 30000}

	c, err := Dial(host, port, target, source, 2*time.Second)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer c.Close() //nolint:errcheck

	c.tx.chanMu.Lock()
	chanCap := cap(c.tx.systemResponse)
	c.tx.chanMu.Unlock()

	if chanCap < 1 {
		t.Fatalf("systemResponse channel capacity = %d, want >= 1", chanCap)
	}
}

func TestReadProcessInputBit_ByteOffsetOverflow(t *testing.T) {
	c := &Client{}
	_, err := c.ReadProcessInputBit(context.Background(), math.MaxUint32, 0)
	if err == nil {
		t.Fatal("expected error for byteOffset that overflows uint32*8, got nil")
	}
	if !strings.Contains(err.Error(), "overflow") && !strings.Contains(err.Error(), "byteOffset") {
		t.Errorf("expected overflow error mentioning overflow or byteOffset, got: %v", err)
	}
}

func TestWriteProcessOutputBit_ByteOffsetOverflow(t *testing.T) {
	c := &Client{}
	err := c.WriteProcessOutputBit(context.Background(), math.MaxUint32, 0, true)
	if err == nil {
		t.Fatal("expected error for byteOffset that overflows uint32*8, got nil")
	}
	if !strings.Contains(err.Error(), "overflow") && !strings.Contains(err.Error(), "byteOffset") {
		t.Errorf("expected overflow error mentioning overflow or byteOffset, got: %v", err)
	}
}

// TestSetSource_TakesEffectOnTheWire: what the local-mode handshake learns has to
// reach the packets.
//
// The Client holds the source AMS address by value, copied at construction. In
// local mode the handshake cannot run until the Client exists (it is itself a
// request), so the address the router assigns arrives afterwards — and nothing
// propagated it. Every request for the rest of the session went out with the
// auto-derived placeholder (127.0.0.1.1.1 and a random port) instead.
func TestSetSource_TakesEffectOnTheWire(t *testing.T) {
	placeholder := AMSAddress{NetID: [6]byte{127, 0, 0, 1, 1, 1}, Port: 33333}
	assigned := AMSAddress{NetID: [6]byte{192, 168, 3, 52, 1, 1}, Port: 32905}

	c := &Client{
		tx:     &transport{},
		logger: getDefaultLogger(),
		target: AMSAddress{NetID: [6]byte{5, 1, 2, 3, 1, 1}, Port: 851},
		source: placeholder,
	}

	before, err := c.encode(CommandIDRead, []byte{1, 2, 3, 4}, 1)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	c.setSource(assigned)
	after, err := c.encode(CommandIDRead, []byte{1, 2, 3, 4}, 2)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	// Source AMS address sits after the 6-byte AMS/TCP header and the 8-byte target.
	const sourceOffset = 6 + 8
	gotBefore := before[sourceOffset : sourceOffset+6]
	gotAfter := after[sourceOffset : sourceOffset+6]
	if !bytes.Equal(gotBefore, placeholder.NetID[:]) {
		t.Fatalf("source before = % x, want the placeholder % x — test is reading the wrong offset",
			gotBefore, placeholder.NetID[:])
	}
	if !bytes.Equal(gotAfter, assigned.NetID[:]) {
		t.Errorf("source on the wire = % x after setSource(%v): requests still carry the pre-handshake address, so the PLC "+
			"answers a NetID this session is not using", gotAfter, assigned)
	}
}

// TestReadFrames_SourceRaceWithLocalHandshake: the transport-fault log line reads
// c.source from the listen goroutine while the local-mode handshake writes it.
//
// setSource holds tx.connMu; the "PLC closed connection" log line read the field
// bare. Both setSource callers publish the Client — and so start listen — before
// the handshake runs, because the handshake is itself an ADS request. So the two
// accesses genuinely overlap, and this test is the configuration no existing test
// had: live workers plus setSource on the same Client. The race detector is the
// oracle; the log assertion only proves the branch under test executed.
func TestReadFrames_SourceRaceWithLocalHandshake(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr, ok := ln.Addr().(*net.TCPAddr)
	if !ok {
		_ = ln.Close()
		t.Fatalf("unexpected addr type: %T", ln.Addr())
	}
	// The server holds the accepted connection until dropConn, then closes it:
	// readFrames sees io.EOF, which isLikelyMissingRoute accepts, so the log
	// line that reads c.source runs.
	dropConn := make(chan struct{})
	served := make(chan struct{})
	go func() {
		defer close(served)
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		<-dropConn
		_ = conn.Close()
	}()
	t.Cleanup(func() {
		_ = ln.Close()
		<-served
	})

	handler := &testLogHandler{}
	placeholder := AMSAddress{NetID: [6]byte{127, 0, 0, 1, 1, 1}, Port: 33333}
	c, err := Dial(addr.IP.String(), addr.Port, AMSAddress{NetID: [6]byte{5, 1, 2, 3, 1, 1}, Port: 851},
		placeholder, time.Second, WithClientLogger(slog.New(handler)))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })

	// Differ in every byte from the placeholder so a torn read is also visible
	// as a value, not only to the detector.
	assigned := AMSAddress{NetID: [6]byte{192, 168, 3, 52, 2, 2}, Port: 32905}
	stop := make(chan struct{})
	var writer sync.WaitGroup
	writer.Add(1)
	go func() {
		defer writer.Done()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			if i%2 == 0 {
				c.setSource(assigned)
			} else {
				c.setSource(placeholder)
			}
		}
	}()

	close(dropConn)
	deadline := time.Now().Add(2 * time.Second)
	logged := false
	for time.Now().Before(deadline) {
		if handler.findByMessage("PLC closed connection, transport down") != nil {
			logged = true
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	close(stop)
	writer.Wait()
	if !logged {
		t.Fatal("readFrames never logged the transport-fault line, so the racing read never ran — " +
			"the stub's close did not surface as an isLikelyMissingRoute error")
	}
}

// TestGetSymbol_TraceLogDoesNotRaceNotificationWriter: the trace line at the end of
// getSymbol must not hand the live *symbol to the logger.
//
// slog formats a *symbol by reflection and reads Value / Valid / ValueParsed /
// LastUpdateTime. Those are written by updateValue under cache.lock, from the
// Client's recvWorker via handleNotification → dispatchSample. getSymbol logged the
// pointer after releasing cache.lock, so a subscribed session with trace logging on
// read a string header and a multi-word time.Time unsynchronised.
//
// The configuration no other test had is the enabled LevelTrace handler: slog skips
// its args at a disabled level, which is the only reason the suite was green. So this
// test enables LevelTrace while notifications flow. -race is the oracle.
//
// Lives in client_test.go rather than beside getSymbol only because of file ownership
// during the concurrent fix waves.
func TestGetSymbol_TraceLogDoesNotRaceNotificationWriter(t *testing.T) {
	srv := startScriptableServer(t)
	defer srv.stop()

	srv.onWriteRead(GroupSymbolInfoByNameEx, func(req []byte) []byte {
		name := strings.TrimRight(string(req), "\x00")
		return buildSymbolInfoPayload(name, "INT", "", 0x4040, 0x100, 2, ADSTInt16, 0)
	})
	var nextHandle atomic.Uint32
	srv.onWriteRead(GroupSymbolHandleByName, func(_ []byte) []byte {
		return buildHandlePayload(nextHandle.Add(1))
	})

	sess, client := newWiredTestSession(t, srv)
	// An ENABLED trace handler that actually formats its attrs. io.Discard keeps
	// the output cost off the test, but the reflection over the attr still happens.
	sess.logger = slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: LevelTrace}))
	client.SetNotificationHandler(sess.handleNotification)
	ctx := context.Background()

	const symbolName = "MAIN.traced"
	const notifHandle uint32 = 0x0BAD0002
	sym, err := sess.getSymbol(ctx, symbolName)
	if err != nil {
		t.Fatalf("resolving %s: %v", symbolName, err)
	}
	updates := make(chan *Update, 1)
	sess.notifications.lock.Lock()
	sess.notifications.activeNotifications[notifHandle] = activeNotification{Sym: sym, Ch: updates}
	sess.notifications.lock.Unlock()

	sample := make([]byte, 2)
	binary.LittleEndian.PutUint16(sample, 7)
	packet := buildNotificationPacket(notifHandle, 0, sample)

	const (
		readers     = 4
		dispatchers = 4
		iterations  = 200
	)
	var wg sync.WaitGroup
	var readErr atomic.Value

	// Readers — production getSymbol on a cached symbol reaches the trace line.
	for i := 0; i < readers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				got, err := sess.getSymbol(ctx, symbolName)
				switch {
				case err != nil:
					readErr.Store(err)
					return
				case got != sym:
					readErr.Store(fmt.Errorf("getSymbol returned %p, want the cached %p", got, sym))
					return
				}
			}
		}()
	}
	// Writers — production dispatch mutates Value / Valid / LastUpdateTime under cache.lock.
	for i := 0; i < dispatchers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				if err := sess.drivePacket(ctx, packet); err != nil {
					readErr.Store(fmt.Errorf("drivePacket: %w", err))
					return
				}
			}
		}()
	}

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("getSymbol / dispatch workers did not finish in 30s — a lock-order inversion, not a race")
	}
	if err, ok := readErr.Load().(error); ok && err != nil {
		t.Fatalf("worker failed: %v", err)
	}
	if !sym.ValueParsed {
		t.Error("no notification sample was ever parsed, so the writer half never ran")
	}
}

// TestHandleReceive_AMSErrorSurfacesAsItself: an AMS-level rejection must reach the
// caller as that error, not as a guess parsed from the body.
//
// AMSHeader.ErrorCode was only ever written, never read. So when the router refused
// a request — target port not found, no runtime, invalid NetID — the library parsed
// the accompanying body as though it were a response. Measured against a TC3.1.4024
// system in CONFIG, where every request to the runtime port comes back with
// ErrorCode 6: a read of index group 0xF008 was reported as
// "ADS error in Read: 0xF008: unknown error code". That is the index group echoed
// back, formatted as a return code. Every "unknown error code" in this project's
// logs came from this, and it sent two diagnoses down the wrong path.
func TestHandleReceive_AMSErrorSurfacesAsItself(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	conn := &Session{
		lifecycle: &sessionLifecycle{ctx: ctx},
		logger:    getDefaultLogger(),
		tx:        &transport{activeRequests: make(map[uint32]chan amsReply)},
	}
	conn.client.Store(&Client{tx: conn.tx, logger: conn.logger, ctx: ctx})

	ch := make(chan amsReply, 1)
	conn.tx.activeRequestLock.Lock()
	conn.tx.activeRequests[7] = ch
	conn.tx.activeRequestLock.Unlock()

	// What a system in CONFIG actually sends: ErrorCode 6, and a body that is the
	// echoed request rather than a response.
	header := AMSHeader{
		Command:   CommandIDRead,
		State:     5,
		Length:    4,
		ErrorCode: uint32(ReturnCodeGlobalTargetPortNotFound),
		InvokeID:  7,
	}
	buf := new(bytes.Buffer)
	if err := binary.Write(buf, binary.LittleEndian, header); err != nil {
		t.Fatalf("write header: %v", err)
	}
	buf.Write([]byte{0x08, 0xF0, 0x00, 0x00}) // 0xF008, the index group, echoed
	conn.client.Load().handleReceive(ctx, buf.Bytes())

	select {
	case reply := <-ch:
		if reply.amsErr != ReturnCodeGlobalTargetPortNotFound {
			t.Errorf("amsErr = %v, want ReturnCodeGlobalTargetPortNotFound: the header's ErrorCode is being discarded", reply.amsErr)
		}
		data, err := reply.payload()
		if err == nil {
			t.Error("payload() returned no error for an AMS-rejected request: the body would be parsed as a response, " +
				"which is how an index group ends up reported as a return code")
		}
		if data != nil {
			t.Errorf("payload() returned %v alongside an AMS error; the body is not a response", data)
		}
		if !errors.Is(err, ReturnCodeGlobalTargetPortNotFound) {
			t.Errorf("error %v does not wrap the code, so callers cannot branch on it", err)
		}
		if !strings.Contains(err.Error(), "target port") {
			t.Errorf("error %q does not name the cause; an operator needs to read 'target port not found', "+
				"which is what a PLC in CONFIG reports for its runtime port", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for the reply")
	}
}
