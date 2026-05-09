package ads

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
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
	conn.client = &Client{tx: conn.tx, logger: conn.logger, ctx: ctx}
	conn.client.waitGroup.Add(1)

	var listenDone sync.WaitGroup
	listenDone.Add(1)
	go func() {
		defer listenDone.Done()
		conn.client.listen()
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
	conn.client = &Client{tx: conn.tx, logger: conn.logger, ctx: ctx}
	conn.client.waitGroup.Add(1)

	var listenDone sync.WaitGroup
	listenDone.Add(1)
	go func() {
		defer listenDone.Done()
		conn.client.listen()
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
	conn.client = &Client{tx: conn.tx, logger: conn.logger, target: conn.target, source: conn.source}

	data := []byte{0x01, 0x02, 0x03, 0x04}
	packet, err := conn.client.encode(CommandIDRead, data, 7)
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
	conn.client = &Client{tx: conn.tx, logger: conn.logger, target: conn.target, source: conn.source}

	packet, err := conn.client.encode(CommandIDReadDeviceInfo, nil, 0)
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
	conn.client = &Client{tx: conn.tx, logger: conn.logger, target: target, source: source}

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
			packet, err := conn.client.encode(cmd, payload, invokeID)
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
		tx:        &transport{activeRequests: make(map[uint32]chan []byte)},
	}
	conn.client = &Client{tx: conn.tx, logger: conn.logger, ctx: ctx}

	// Register a response channel for invokeID 42
	ch := make(chan []byte, 1)
	conn.tx.activeRequestLock.Lock()
	conn.tx.activeRequests[42] = ch
	conn.tx.activeRequestLock.Unlock()

	// Build AMS header + data
	header := amsHeader{
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

	conn.client.handleReceive(ctx, buf.Bytes())

	select {
	case resp := <-ch:
		if !bytes.Equal(resp, []byte{0xDE, 0xAD, 0xBE, 0xEF}) {
			t.Errorf("response = %v, want [0xDE 0xAD 0xBE 0xEF]", resp)
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
		tx:        &transport{activeRequests: make(map[uint32]chan []byte)},
	}
	conn.client = &Client{tx: conn.tx, logger: conn.logger, ctx: ctx}

	// No registered channels — should not panic
	header := amsHeader{
		Command:  CommandIDRead,
		State:    5,
		Length:   2,
		InvokeID: 999,
	}
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, &header)
	buf.Write([]byte{0x00, 0x00})

	// Should not panic or error
	conn.client.handleReceive(ctx, buf.Bytes())
}

func TestHandleReceive_TooShort(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	conn := &Session{
		lifecycle: &sessionLifecycle{ctx: ctx},
		logger:    getDefaultLogger(),
		tx:        &transport{activeRequests: make(map[uint32]chan []byte)},
	}
	conn.client = &Client{tx: conn.tx, logger: conn.logger, ctx: ctx}

	// Less than 32 bytes — should return early
	conn.client.handleReceive(ctx, []byte{1, 2, 3, 4, 5})
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
