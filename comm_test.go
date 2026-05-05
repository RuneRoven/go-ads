package ads

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"syscall"
	"testing"
)

// Verify sendRequestOnce ctxDone branch returns context.Canceled (not ErrDisconnected),
// so the outer sendRequest retry loop re-engages.
//
// Simulates the disconnected-wait state by setting up a Connection with
// disconnected=true and a non-nil reconnectDone channel, then cancelling the ctx.
func TestSendRequestOnce_CtxDoneReturnsCanceled(t *testing.T) {
	conn := &Connection{
		logger:        getDefaultLogger(),
		reconnectDone: make(chan struct{}),
	}
	ctx, cancel := context.WithCancel(context.Background())
	conn.ctx = ctx
	conn.shutdown = cancel
	conn.disconnected.Store(true)

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
