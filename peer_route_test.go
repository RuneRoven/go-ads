package ads

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"strconv"
	"strings"
	"sync"
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
	isolatePeerRouteCache(t)
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
	isolatePeerRouteCache(t)
	srv := startScriptableServer(t)
	defer srv.stop()

	srv.onRead(GroupSymbolVersion, func(_, _, _ uint32) (ReturnCode, []byte) {
		return ReturnCodeNoErrors, []byte{7}
	})
	port := freeLocalPort(t)
	srv.answerViaPeerConnection(localAddr(port)) // nothing listens there

	// Its own port, not the protocol default: this test used to bind
	// 0.0.0.0:48898 — the host's real AMS port — as a side effect.
	fallbackPort := freeLocalPort(t)
	logs := &testLogHandler{}
	sess, err := NewSession(context.Background(),
		AMSEndpoint{IP: srv.host, Port: srv.port, AMS: AMSAddress{NetID: [6]byte{5, 1, 2, 3, 1, 1}, Port: 851}},
		WithRequestTimeout(300*time.Millisecond),
		WithTargetCheck(TargetCheckOff),
		WithAutoReconnect(false),
		WithLogger(slog.New(logs)),
		WithAmsPeerListen(fallbackPort),
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
	// The fallback must have RUN and failed honestly, not been skipped. Asserting
	// only "Connect failed" passed even with tryPeerFallback gutted to an
	// unconditional error, and would also pass on a host where the port is simply
	// taken.
	//
	// Evidence is the bind log line, not a surviving sess.peerLn: a failed Connect
	// releases the listener now (see the defer in Connect), so the live field says
	// nothing about whether the fallback ran.
	if rec := logs.findByMessage("listening for inbound PLC connections"); rec == nil {
		t.Error("no listener was ever bound: the fallback did not run, so this test would pass with it deleted")
	}
	if !strings.Contains(err.Error(), "answered neither GetSymbolVersion nor ReadState") {
		t.Errorf("error %q does not report what was tried", err)
	}
}

// TestPeerRoute_CloseDoesNotHangWithAdoptedConnection: an adopted inbound
// connection has its own reader goroutine, blocked in Read on a socket that
// nothing else touches. Teardown waits on the Client's waitGroup, so unless those
// sockets are closed first the wait never returns — observed hanging a real
// session against .224 after the route-table probe finished.
func TestPeerRoute_CloseDoesNotHangWithAdoptedConnection(t *testing.T) {
	isolatePeerRouteCache(t)
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

// isolatePeerRouteCache gives one test the process-wide peerRouteHosts cache to
// itself, and hands it back exactly as it was found.
//
// peerRouteHosts is a package-level sync.Map keyed by "host:port" that lives for
// the whole process, and nothing in a test binary ever expires it. Its
// self-invalidation (forgetPeerRouteHostIfUnused) only fires for a device that
// answered without an inbound connection, which is exactly what a RESCUED stub did
// not do — so every rescued session still leaves an entry behind, by design:
// measured at two per run of this file, from
// TestPeerRoute_AutomaticFallback and TestPeerRoute_FallbackCanBeDisabled's first
// arm — and every stub in the binary lives on 127.0.0.1, so a later stub that the
// OS hands a recycled ephemeral port INHERITS that entry.
//
// The consequence is not subtle, and is not confined to this file. Connect
// pre-binds the inbound listener for a host it believes needs one, and with no
// WithAmsPeerListen that listener is wildcard :48898. Measured: seeding one entry
// for a stub's own address makes a session against a perfectly HEALTHY device
// seize :48898 for its entire lifetime — which is then the port these two tests
// need. Nothing in the failing test names the cause, and the trigger is a port
// collision, so it reproduces roughly never.
//
// Clearing on the way in as well as on the way out is deliberate: restoring alone
// would stop this file from poisoning others, but would leave these tests at the
// mercy of residue from whatever ran before them.
func isolatePeerRouteCache(t *testing.T) {
	t.Helper()
	inherited := map[string]struct{}{}
	peerRouteHosts.Range(func(k, _ any) bool {
		if key, ok := k.(string); ok {
			inherited[key] = struct{}{}
			peerRouteHosts.Delete(key)
		}
		return true
	})
	t.Cleanup(func() {
		peerRouteHosts.Range(func(k, _ any) bool {
			if key, ok := k.(string); ok {
				peerRouteHosts.Delete(key)
			}
			return true
		})
		for key := range inherited {
			rememberPeerRouteHost(key)
		}
	})
}

// TestPeerRoute_HealthyDeviceDropsTheRememberedHost: a session that gets its
// answers on its OWN connection must drop the remembered peer-route fact for that
// device, whether or not it registered a route.
//
// forgetPeerRouteHost was reachable only from Connect's route-registration branch,
// so a caller that does not use WithRoute never invalidated anything: once a device
// was remembered, every later session in the process pre-bound the inbound AMS port
// — wildcard :48898 by default — for the rest of the process's life, even after the
// device's route table had been repaired. That is the configuration umh-core runs in
// a container, and the doc comment on forgetPeerRouteHost claimed the entry
// "self-invalidates as soon as an ordinary probe succeeds", which it did not.
//
// Two assertions, because the entry is not the harm: the harm is the port it makes
// the NEXT session seize.
func TestPeerRoute_HealthyDeviceDropsTheRememberedHost(t *testing.T) {
	isolatePeerRouteCache(t)
	srv := startScriptableServer(t)
	defer srv.stop()

	// A healthy device: it answers on the connection we opened, and never dials us.
	srv.onRead(GroupSymbolVersion, func(_, _, _ uint32) (ReturnCode, []byte) {
		return ReturnCodeNoErrors, []byte{5}
	})

	// Seed the fact, as a genuine peer-route device (or a stub on a recycled port)
	// would have. Deliberately no WithRoute anywhere in this test — that is the path
	// that had no invalidation at all.
	key := net.JoinHostPort(srv.host, strconv.Itoa(srv.port))
	rememberPeerRouteHost(key)

	// Deliberately no WithAmsPeerListen: that option binds unconditionally, so it
	// would hide the very thing under test. The pre-bind this exercises is the
	// implicit one, on the wildcard protocol port.
	if perr := protocolPortIsBindable(t); perr != nil {
		t.Skipf("port %d unavailable on this host (%v); the pre-bind cannot be observed", amsPeerListenPort, perr)
	}
	connect := func(t *testing.T) (*Session, *testLogHandler) {
		t.Helper()
		logs := &testLogHandler{}
		sess, err := NewSession(context.Background(),
			AMSEndpoint{IP: srv.host, Port: srv.port, AMS: AMSAddress{NetID: [6]byte{5, 1, 2, 3, 1, 1}, Port: 851}},
			WithRequestTimeout(500*time.Millisecond),
			WithTargetCheck(TargetCheckOff),
			WithAutoReconnect(false),
			WithLogger(slog.New(logs)),
		)
		if err != nil {
			t.Fatalf("NewSession: %v", err)
		}
		t.Cleanup(func() { sess.Close() })
		if err := sess.Connect(context.Background()); err != nil {
			skipIfPortTaken(t, err)
			t.Fatalf("Connect against a healthy device: %v", err)
		}
		return sess, logs
	}

	first, firstLogs := connect(t)
	// The seeded entry must actually have cost this session a bound port, or the
	// assertion below would hold with the whole mechanism deleted.
	if firstLogs.findByMessage("device is known to answer on its own connection") == nil {
		t.Fatal("the seeded entry was never acted on, so this test proves nothing about invalidating it")
	}
	if isKnownPeerRouteHost(key) {
		t.Error("the device answered on our own connection, yet it is still remembered as needing an inbound listener: every later session in this process now pre-binds the AMS port for nothing")
	}
	// Hand the port back before the second session, so a pre-bind there is visible
	// as a bind rather than as a collision with this one.
	_ = first.Close()

	second, _ := connect(t)
	second.peerMu.Lock()
	ln := second.peerLn
	second.peerMu.Unlock()
	if ln != nil {
		t.Errorf("the next session still bound the inbound AMS port (%v) against a healthy device", ln.Addr())
	}
}

// protocolPortIsBindable reports whether the fallback could bind the AMS port.
//
// Wildcard, because that is what startPeerListener binds. A 127.0.0.1 probe is
// worse than no probe at all: on macOS SO_REUSEADDR lets a loopback bind succeed
// while another socket in this very process holds :48898 wildcard, so the guard
// reports "clear", the test proceeds, and the bind it was guarding then fails.
// Measured directly — 127.0.0.1:48898 accepted a listener while :48898 was held.
func protocolPortIsBindable(t *testing.T) error {
	t.Helper()
	probe, err := net.Listen("tcp4", net.JoinHostPort("", strconv.Itoa(amsPeerListenPort)))
	if err != nil {
		return err
	}
	return probe.Close()
}

// skipIfPortTaken turns a lost race for the protocol port into a skip.
//
// The fallback binds 48898 by definition, and the port is a single system-wide
// resource: an integration run against a peer-route device holds it for the whole
// run, and these tests share the binary. Probing the port up front is not enough,
// because it can be taken between the probe and the bind — which is exactly what
// happened during a sweep against 192.168.3.224.
//
// This is the one coupling in this file that cannot be engineered away.
// TestPeerRoute_AutomaticFallback and TestPeerRoute_FallbackCanBeDisabled bind
// :48898 for real, because refusing to bind it is the behaviour under test, and
// a port is a property of the machine rather than of the test. The two are
// therefore mutually exclusive with each other, with any other binder of :48898
// in this binary, and with a local TwinCAT router — so they must stay serial, and
// must be excluded from any future t.Parallel() sweep. Every other test in this
// file binds only an ephemeral port from freeLocalPort and has no such
// constraint.
func skipIfPortTaken(t *testing.T, err error) {
	t.Helper()
	if err != nil && strings.Contains(err.Error(), "address already in use") {
		t.Skipf("port %d is held by something else (likely a concurrent integration run against a peer-route device): %v",
			amsPeerListenPort, err)
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
	isolatePeerRouteCache(t)
	// The fallback binds the protocol port, so the stub has to answer there.
	if err := protocolPortIsBindable(t); err != nil {
		t.Skipf("port %d unavailable on this host (%v); the fallback cannot be exercised", amsPeerListenPort, err)
	}

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
		skipIfPortTaken(t, err)
		t.Fatalf("Connect did not fall back to listening: %v", err)
	}
	if rec := logs.findByMessage("answers on a connection it opens to us"); rec == nil {
		if logs.findByMessage("address already in use") != nil {
			t.Skip("the protocol port was taken mid-test by something else in this binary")
		}
		t.Error("no log line telling the operator the fallback was used")
	} else if rec.Level < slog.LevelWarn {
		t.Errorf("fallback logged at %v; it should be at least Warn so it is not missed", rec.Level)
	}
	// The other half of forgetPeerRouteHostIfUnused: learning this costs a full
	// probe timeout plus the route-activation budget, so a device that really does
	// answer on its own connection must stay remembered. Without this, invalidating
	// on every successful Connect would look correct and quietly re-learn the same
	// ~15s fact for every session.
	if !isKnownPeerRouteHost(net.JoinHostPort(srv.host, strconv.Itoa(srv.port))) {
		t.Error("the rescued device was not remembered, so every later session in this process pays the discovery cost again")
	}
}

// TestPeerRoute_FallbackCanBeDisabled: hosts that already run a TwinCAT router own
// the AMS port, so binding it must be refusable — and refusing it must be the ONLY
// difference from the default.
//
// Two arms in one test on purpose. Asserting only "Connect failed and the message
// names the option" passed even with tryPeerFallback gutted to an unconditional
// error, and asserting "no socket was bound" does not help either, because a gutted
// fallback binds nothing too. The contrast is what discriminates: the same silent
// device must be rescued without the option and refused with it.
func TestPeerRoute_FallbackCanBeDisabled(t *testing.T) {
	isolatePeerRouteCache(t)
	// The default arm binds the protocol port, so skip if this host cannot.
	if perr := protocolPortIsBindable(t); perr != nil {
		t.Skipf("port %d unavailable on this host (%v); the contrast cannot be exercised", amsPeerListenPort, perr)
	}

	newSilentDevice := func(t *testing.T) *scriptableServer {
		t.Helper()
		srv := startScriptableServer(t)
		t.Cleanup(srv.stop)
		srv.onRead(GroupSymbolVersion, func(_, _, _ uint32) (ReturnCode, []byte) {
			return ReturnCodeNoErrors, []byte{9}
		})
		// Answers only on a connection it opens to us, on the protocol port.
		srv.answerViaPeerConnection(localAddr(amsPeerListenPort))
		return srv
	}
	connect := func(t *testing.T, srv *scriptableServer, extra ...SessionOption) (*Session, error) {
		t.Helper()
		opts := append([]SessionOption{
			WithRequestTimeout(500 * time.Millisecond),
			WithTargetCheck(TargetCheckOff),
			WithAutoReconnect(false),
		}, extra...)
		sess, err := NewSession(context.Background(),
			AMSEndpoint{IP: srv.host, Port: srv.port, AMS: AMSAddress{NetID: [6]byte{5, 1, 2, 3, 1, 1}, Port: 851}},
			opts...)
		if err != nil {
			t.Fatalf("NewSession: %v", err)
		}
		t.Cleanup(func() { sess.Close() })
		return sess, sess.Connect(context.Background())
	}

	// Arm A — default: the fallback rescues this device.
	rescued, err := connect(t, newSilentDevice(t))
	if err != nil {
		skipIfPortTaken(t, err)
		t.Fatalf("default Connect did not rescue a peer-route device: %v", err)
	}
	rescued.peerMu.Lock()
	rescuedLn := rescued.peerLn
	rescued.peerMu.Unlock()
	if rescuedLn == nil {
		t.Error("rescued without binding a listener — then the rescue came from somewhere else and this test proves nothing")
	}

	// Hand :48898 back before arm B runs. Leaving arm A listening made arm B's
	// refusal over-determined: arm A's accept loop swallowed arm B's stub's
	// dial-back, so arm B saw silence whatever the option did, and if the option
	// had been ignored arm B's own bind would merely have failed with "address
	// already in use" — an error that reads like a busy host, not like a bug.
	// Released, arm B's stub gets ECONNREFUSED, and an ignored WithoutAmsPeerFallback
	// binds successfully and RESCUES arm B, which the assertions below then catch.
	// Close is idempotent, so the t.Cleanup above still fires harmlessly.
	_ = rescued.Close()

	// Arm B — opted out: the same device must be refused, and nothing bound.
	refused, err := connect(t, newSilentDevice(t), WithoutAmsPeerFallback())
	if err == nil {
		t.Fatal("Connect succeeded with the fallback disabled")
	}
	if !strings.Contains(err.Error(), "WithoutAmsPeerFallback") {
		t.Errorf("error does not say the fallback was disabled: %v", err)
	}
	refused.peerMu.Lock()
	refusedLn := refused.peerLn
	refused.peerMu.Unlock()
	if refusedLn != nil {
		t.Error("a listener was bound although the fallback is disabled")
	}
}

// TestPeerRoute_AdoptionAfterTeardownIsRefused: once the adopted connections have
// been dropped for a teardown, the Client must refuse any further one.
//
// A peer-route device dials us again after every drop, which is precisely when
// teardown runs. tearDownAndReset does closePeerConns() and then waits on the
// Client's WaitGroup, so an adoption landing in between is three bugs at once: the
// connection is appended to a slice that was just nil'd so nobody ever closes it
// (one leaked fd per reconnect, and a half-open socket the PLC counts against its
// one-connection-per-IP limit), its reader blocks in io.ReadFull so the wait never
// returns, and the WaitGroup.Add races a Wait that may already be at zero, which
// is documented misuse and panics the process.
func TestPeerRoute_AdoptionAfterTeardownIsRefused(t *testing.T) {
	srv := startScriptableServer(t)
	defer srv.stop()

	c, err := Dial(srv.host, srv.port, AMSAddress{}, AMSAddress{}, 2*time.Second)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })

	// Teardown has begun: the adopted connections were dropped and the caller is
	// about to wait for the readers.
	c.closePeerConns()

	inbound, err := net.Dial("tcp", localAddr(srv.port))
	if err != nil {
		t.Fatalf("dial inbound: %v", err)
	}
	defer func() { _ = inbound.Close() }()

	c.AcceptPeerConn(inbound)

	c.peerMu.Lock()
	adopted := len(c.peerConns)
	c.peerMu.Unlock()
	if adopted != 0 {
		t.Errorf("peerConns holds %d connection(s) after closePeerConns: appended to a slice nobody will close again", adopted)
	}

	// The refusal has to close the socket, or it leaks: our end stays readable
	// forever instead of seeing the peer hang up.
	_ = inbound.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 1)
	_, rerr := inbound.Read(buf)
	if rerr == nil {
		t.Fatal("read succeeded on a connection that should have been refused and closed")
	}
	var ne net.Error
	if errors.As(rerr, &ne) && ne.Timeout() {
		t.Error("the refused connection was left open (read timed out instead of seeing EOF): " +
			"its reader is still blocked in io.ReadFull, which is what hangs tearDownAndReset's wait")
	}
}

// TestPeerRoute_AdoptedConnectionIsClosedWhenItsReaderExits: the reader owns the
// socket it was handed.
//
// readFrames returns on ctx.Done() and on any read error without touching conn,
// and nothing pruned peerConns, so every connection the PLC ever opened cost an fd
// and a slice entry for the life of the Client. A device that re-dials on each of
// its own drops accumulates them.
func TestPeerRoute_AdoptedConnectionIsClosedWhenItsReaderExits(t *testing.T) {
	srv := startScriptableServer(t)
	defer srv.stop()

	c, err := Dial(srv.host, srv.port, AMSAddress{}, AMSAddress{}, 2*time.Second)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })

	ours, theirs := net.Pipe()
	c.AcceptPeerConn(theirs)
	c.peerMu.Lock()
	adopted := len(c.peerConns)
	c.peerMu.Unlock()
	if adopted != 1 {
		t.Fatalf("peerConns = %d after a live adoption, want 1", adopted)
	}

	// End the reader the way a PLC hanging up does.
	_ = ours.Close()

	deadline := time.Now().Add(3 * time.Second)
	for {
		c.peerMu.Lock()
		n := len(c.peerConns)
		c.peerMu.Unlock()
		if n == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("peerConns still holds %d entry after its reader exited: entries are never pruned, so they accumulate for the life of the Client", n)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestPeerListener_RetryAfterAFailedBind: a bind failure must not be latched.
//
// The listener used sync.Once, which runs its body once whether it succeeded or
// not, and kept the error in a closure local — so after a failed bind every later
// call returned nil with nothing listening. A Connect retry then believed the
// listener was up, and tryPeerFallback went on to announce "listening for one it
// may open to us" and probe a port it had never bound, swallowing the one hint that
// names the real cause.
func TestPeerListener_RetryAfterAFailedBind(t *testing.T) {
	port := freeLocalPort(t)
	// Bind the same address the listener will: on macOS occupying 127.0.0.1:port
	// does not conflict with a wildcard bind, so the block has to be wildcard too.
	blocker, err := net.Listen("tcp4", net.JoinHostPort("", strconv.Itoa(port)))
	if err != nil {
		t.Fatalf("occupy the port: %v", err)
	}

	sess := &Session{
		logger:    getDefaultLogger(),
		lifecycle: &sessionLifecycle{closedCh: make(chan struct{})},
	}
	sess.peerListenPort = port
	t.Cleanup(func() { sess.stopPeerListener() })

	if err := sess.startPeerListener(); err == nil {
		t.Fatal("bind succeeded although the port was occupied")
	}

	// The port frees up — as it does when the local TwinCAT router is stopped, or
	// simply between two Connect attempts.
	_ = blocker.Close()

	if err := sess.startPeerListener(); err != nil {
		t.Fatalf("second attempt refused after the port was freed: %v", err)
	}
	sess.peerMu.Lock()
	ln := sess.peerLn
	sess.peerMu.Unlock()
	if ln == nil {
		t.Error("startPeerListener reported success with no listener: the failed bind was latched, so every later caller " +
			"is told it is listening when nothing is")
	}
}

// TestConnect_ReleasesPeerListenerOnFailure: a Connect that fails must not leave
// the inbound AMS listener bound.
//
// Connect binds it before dialing (WithAmsPeerListen), and the rollback only
// restored the FSM — so every failed attempt left the port held plus its accept
// loop running. Callers respond to a Connect error by discarding the session and
// building a new one, never by calling Close, so with a retry loop the leak is
// unbounded: measured as one held port and ~19 surviving goroutines per attempt.
//
// The release must be non-latching, or the documented retry (the FSM rolls back to
// Disconnected, and Disconnected -> Connecting is legal) can never bind again —
// hence the second arm.
func TestConnect_ReleasesPeerListenerOnFailure(t *testing.T) {
	isolatePeerRouteCache(t)
	port := freeLocalPort(t)
	ep := testEndpoint()
	// Nothing serves TCP port 1 on loopback, so the dial fails immediately: this
	// test must never depend on a timeout.
	ep.Port = 1

	sess, err := NewSession(context.Background(), ep,
		WithRequestTimeout(500*time.Millisecond),
		WithTargetCheck(TargetCheckOff),
		WithAutoReconnect(false),
		WithAmsPeerListen(port),
	)
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	t.Cleanup(func() { sess.Close() })

	if err := sess.Connect(context.Background()); err == nil {
		t.Fatal("Connect succeeded against a closed port")
	}

	sess.peerMu.Lock()
	ln, stopped := sess.peerLn, sess.peerStopped
	sess.peerMu.Unlock()
	if ln != nil {
		t.Error("the inbound listener is still bound after a failed Connect: the port and its accept loop leak per attempt")
	}
	// Independent of the field: the port itself has to be free again, and the accept
	// loop gone. A wildcard bind, because that is what startPeerListener uses — on
	// macOS a 127.0.0.1 bind does not conflict with a wildcard one.
	rebind, err := net.Listen("tcp4", net.JoinHostPort("", strconv.Itoa(port)))
	if err != nil {
		t.Fatalf("port %d is still bound after a failed Connect: %v", port, err)
	}
	_ = rebind.Close()

	// Second arm: released, not latched. A retry has to be able to bind.
	if stopped {
		t.Fatal("peerStopped was latched by a failed Connect: every retry will be refused with " +
			"\"session is shutting down\" instead of binding")
	}
	if err := sess.startPeerListener(); err != nil {
		t.Fatalf("a retry cannot bind the inbound port after a failed Connect: %v", err)
	}
}

// TestPeerListener_StopDoesNotHangWhenRacingStart: Close must not deadlock against
// a Connect that is bringing the listener up.
//
// peerLn was a plain field written inside the Once, and Close reads it from another
// goroutine with no happens-before edge. Reading nil made Close skip closing the
// listener and then block forever in peerWG.Wait() — the accept loop is parked in
// Accept, and its isClosed() escape only runs once a connection arrives, which on a
// dead PLC never happens. Run with -race, this also pins the race itself.
func TestPeerListener_StopDoesNotHangWhenRacingStart(t *testing.T) {
	for i := 0; i < 20; i++ {
		sess := &Session{
			logger:    getDefaultLogger(),
			lifecycle: &sessionLifecycle{closedCh: make(chan struct{})},
		}
		sess.peerListenPort = freeLocalPort(t)

		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); _ = sess.startPeerListener() }()
		go func() { defer wg.Done(); sess.stopPeerListener() }()

		done := make(chan struct{})
		go func() { wg.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatalf("iteration %d: stopPeerListener hung — it read peerLn as nil, skipped the close, and waited on an accept loop nothing will wake", i)
		}
		sess.stopPeerListener() // idempotent, and cleans up if start won the race
	}
}
