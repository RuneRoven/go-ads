package ads

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"syscall"
	"testing"
	"time"
)

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
		lifecycle: &reconnector{},
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
		lifecycle: &reconnector{closedCh: make(chan struct{})},
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

// Verify sendRequestOnce ctxDone branch returns context.Canceled (not ErrDisconnected),
// so the outer sendRequest retry loop re-engages.
//
// Simulates the disconnected-wait state by setting up a Session with
// disconnected=true and a non-nil reconnectDone channel, then cancelling the ctx.
func TestSendRequestOnce_CtxDoneReturnsCanceled(t *testing.T) {
	conn := &Session{
		logger:    getDefaultLogger(),
		lifecycle: &reconnector{reconnectDone: make(chan struct{})},
		tx:        &transport{},
	}
	ctx, cancel := context.WithCancel(context.Background())
	conn.lifecycle.ctx = ctx
	conn.lifecycle.shutdown = cancel
	conn.client = &Client{tx: conn.tx, logger: conn.logger, ctx: ctx}
	conn.lifecycle.disconnected.Store(true)
	// Walk the FSM into Disconnected so isDisconnected() reflects the synthetic
	// state. Constructed -> Disconnected is not a legal direct transition.
	conn.lifecycle.state.transitionTo(SessionStateConnecting)
	conn.lifecycle.state.transitionTo(SessionStateConnected)
	conn.lifecycle.state.transitionTo(SessionStateDisconnected)

	cancel()

	_, err := conn.sendRequestOnce(CommandIDRead, []byte{})
	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled, got %v (errors.Is(context.Canceled)=%v)",
			err, errors.Is(err, context.Canceled))
	}
}

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
			got := isRouteHintErr(tc.err)
			if got != tc.want {
				t.Errorf("isRouteHintErr(%v) = %v, want %v", tc.err, got, tc.want)
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
	if !isRouteHintErr(wrapped) {
		t.Errorf("isRouteHintErr did not detect ECONNRESET in wrapped *net.OpError")
	}
}
