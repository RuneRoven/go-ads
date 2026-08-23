package ads

import (
	"context"
	"log/slog"
	"net"
	"strings"
	"testing"
	"time"
)

// peer_route_test.go — talking to a device that answers on its OWN connection.
//
// Measured on a TC3.1.4026 TC/RTOS device (192.168.3.224): it accepts our TCP
// connection, accepts and PROCESSES our requests, and then sends every response
// over a connection it opens back to us on 48898. Decoded from the wire:
//
//	target 192.168.3.52.1.1:10900   source 5.154.236.19.1.1:851
//	command 0x0002 Read   state 0x0005 (ADS command + response)   invokeID 1
//
// i.e. the answer to our own first GetSymbolVersion. Nothing was ever lost; a
// client-only library simply has nowhere to receive it, so every request appears
// to time out. TC2 2.10 and TC3.1.4024/CE on the same network answer on our
// connection and never dial back, so this is per-device route-entry behaviour.
//
// Registration cannot ask for anything different: Beckhoff's own AddRemoteRoute
// sends the same five tags we do (computerName, password, username, netid,
// routename) with no route-type or unidirectional flag, and their Linux AdsLib
// never binds a listening socket — so it cannot talk to a device in this state at
// all. Accepting the inbound connection is therefore a capability, not a
// workaround for a mistake of ours.

// TestPeerRoute_ResponsesOnInboundConnection: with the peer listener enabled, a
// device that answers only on its own connection must work normally.
func TestPeerRoute_ResponsesOnInboundConnection(t *testing.T) {
	srv := startScriptableServer(t)
	defer srv.stop()

	srv.onRead(GroupSymbolVersion, func(_, _, _ uint32) (ReturnCode, []byte) {
		return ReturnCodeNoErrors, []byte{42}
	})

	// Pick the port the "PLC" will dial back on, and point the stub at it.
	port := freeLocalPort(t)
	srv.answerViaPeerConnection(localAddr(port))

	sess, err := NewSession(context.Background(),
		AMSEndpoint{IP: srv.host, Port: srv.port, AMS: AMSAddress{NetID: [6]byte{5, 1, 2, 3, 1, 1}, Port: 851}},
		WithRequestTimeout(2*time.Second),
		WithTargetCheck(TargetCheckOff),
		WithAutoReconnect(false),
		WithAmsPeerListen(port),
	)
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	t.Cleanup(func() { sess.Close() })

	if err := sess.Connect(context.Background()); err != nil {
		t.Fatalf("Connect against a device that answers on its own connection: %v", err)
	}

	// And a plain request must work, not just the connect-time probe.
	v, err := sess.client.Load().GetSymbolVersion(context.Background())
	if err != nil {
		t.Fatalf("GetSymbolVersion over the peer connection: %v", err)
	}
	if v != 42 {
		t.Errorf("symbol version = %d, want 42", v)
	}
}

// TestPeerRoute_DisabledStillFailsClearly: when the fallback cannot rescue the
// session — here because the device answers on a port that is not the one the
// fallback binds — Connect must fail with a diagnosis rather than a bare timeout,
// and must not leave a session reporting healthy.
func TestPeerRoute_DisabledStillFailsClearly(t *testing.T) {
	srv := startScriptableServer(t)
	defer srv.stop()

	srv.onRead(GroupSymbolVersion, func(_, _, _ uint32) (ReturnCode, []byte) {
		return ReturnCodeNoErrors, []byte{7}
	})
	port := freeLocalPort(t)
	srv.answerViaPeerConnection(localAddr(port)) // nothing listens there

	sess, err := NewSession(context.Background(),
		AMSEndpoint{IP: srv.host, Port: srv.port, AMS: AMSAddress{NetID: [6]byte{5, 1, 2, 3, 1, 1}, Port: 851}},
		WithRequestTimeout(300*time.Millisecond),
		WithTargetCheck(TargetCheckOff),
		WithAutoReconnect(false),
	)
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	t.Cleanup(func() { sess.Close() })

	err = sess.Connect(context.Background())
	if err == nil {
		t.Fatal("Connect reported success although no response can reach us")
	}
	if sess.lifecycle.state.load() == SessionStateConnected {
		t.Error("session left in Connected after a failed connect")
	}
}

// TestPeerRoute_CloseDoesNotHangWithAdoptedConnection: an adopted inbound
// connection has its own reader goroutine, blocked in Read on a socket that
// nothing else touches. Teardown waits on the Client's waitGroup, so unless those
// sockets are closed first the wait never returns — observed hanging a real
// session against .224 after the route-table probe finished.
func TestPeerRoute_CloseDoesNotHangWithAdoptedConnection(t *testing.T) {
	srv := startScriptableServer(t)
	defer srv.stop()
	srv.onRead(GroupSymbolVersion, func(_, _, _ uint32) (ReturnCode, []byte) {
		return ReturnCodeNoErrors, []byte{5}
	})

	port := freeLocalPort(t)
	srv.answerViaPeerConnection(localAddr(port))

	sess, err := NewSession(context.Background(),
		AMSEndpoint{IP: srv.host, Port: srv.port, AMS: AMSAddress{NetID: [6]byte{5, 1, 2, 3, 1, 1}, Port: 851}},
		WithRequestTimeout(2*time.Second),
		WithTargetCheck(TargetCheckOff),
		WithAutoReconnect(false),
		WithAmsPeerListen(port),
	)
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	if err := sess.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}

	done := make(chan struct{})
	go func() { _ = sess.Close(); close(done) }()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("Close hung with an adopted inbound connection open")
	}
}

// TestPeerRoute_AutomaticFallback: with no option set at all, a Connect that
// proves total silence must bind the AMS port, discover the device is answering
// there, and carry on — announcing it, because a device in this state is worth an
// operator's attention.
//
// This is the path that matters in production: the benthos plugin only ever calls
// Connect, so a session that needs this has no way to ask for it.
func TestPeerRoute_AutomaticFallback(t *testing.T) {
	// The fallback binds the protocol port, so the stub has to answer there.
	probe, err := net.Listen("tcp4", localAddr(amsPeerListenPort))
	if err != nil {
		t.Skipf("port %d unavailable on this host (%v); the fallback cannot be exercised", amsPeerListenPort, err)
	}
	_ = probe.Close()

	srv := startScriptableServer(t)
	defer srv.stop()
	srv.onRead(GroupSymbolVersion, func(_, _, _ uint32) (ReturnCode, []byte) {
		return ReturnCodeNoErrors, []byte{9}
	})
	srv.answerViaPeerConnection(localAddr(amsPeerListenPort))

	logs := &testLogHandler{}
	sess, err := NewSession(context.Background(),
		AMSEndpoint{IP: srv.host, Port: srv.port, AMS: AMSAddress{NetID: [6]byte{5, 1, 2, 3, 1, 1}, Port: 851}},
		WithRequestTimeout(500*time.Millisecond),
		WithTargetCheck(TargetCheckOff),
		WithAutoReconnect(false),
		WithLogger(slog.New(logs)),
		// deliberately no WithAmsPeerListen
	)
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	t.Cleanup(func() { sess.Close() })

	if err := sess.Connect(context.Background()); err != nil {
		t.Fatalf("Connect did not fall back to listening: %v", err)
	}
	if rec := logs.findByMessage("answers on a connection it opens to us"); rec == nil {
		t.Error("no log line telling the operator the fallback was used")
	} else if rec.Level < slog.LevelWarn {
		t.Errorf("fallback logged at %v; it should be at least Warn so it is not missed", rec.Level)
	}
}

// TestPeerRoute_FallbackCanBeDisabled: hosts that already run a TwinCAT router
// own the AMS port, so binding it must be refusable.
func TestPeerRoute_FallbackCanBeDisabled(t *testing.T) {
	srv := startScriptableServer(t)
	defer srv.stop()
	srv.onRead(GroupSymbolVersion, func(_, _, _ uint32) (ReturnCode, []byte) {
		return ReturnCodeNoErrors, []byte{9}
	})
	srv.answerViaPeerConnection(localAddr(amsPeerListenPort))

	sess, err := NewSession(context.Background(),
		AMSEndpoint{IP: srv.host, Port: srv.port, AMS: AMSAddress{NetID: [6]byte{5, 1, 2, 3, 1, 1}, Port: 851}},
		WithRequestTimeout(300*time.Millisecond),
		WithTargetCheck(TargetCheckOff),
		WithAutoReconnect(false),
		WithoutAmsPeerFallback(),
	)
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	t.Cleanup(func() { sess.Close() })

	err = sess.Connect(context.Background())
	if err == nil {
		t.Fatal("Connect succeeded with the fallback disabled")
	}
	if !strings.Contains(err.Error(), "WithoutAmsPeerFallback") {
		t.Errorf("error does not say the fallback was disabled: %v", err)
	}
}
