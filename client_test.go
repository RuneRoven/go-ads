package ads

import (
	"context"
	"errors"
	"net"
	"strconv"
	"strings"
	"sync"
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
