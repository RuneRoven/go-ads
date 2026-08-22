package ads

import (
	"log/slog"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"
)

// transport_fault_level_test.go — the handshake log-level gating.
//
// A cold start is probe → PLC rejects an unknown NetID → register route →
// redial. Every fault in that sequence is an expected state, not a failure, and
// downstream log-based health checks fail a component on any single ERROR line
// in their window. So the gating is only as good as its least-covered site:
// one un-gated transport fault re-breaks the whole thing, which is exactly what
// happened the first time (four sites gated, six missed).

// TestTransportFaultLevel covers the switch itself.
func TestTransportFaultLevel(t *testing.T) {
	c := &Client{tx: &transport{}, dropped: make(chan struct{})}
	if got := c.transportFaultLevel(); got != slog.LevelError {
		t.Errorf("level outside a handshake = %v, want Error", got)
	}
	c.beginHandshake()
	if got := c.transportFaultLevel(); got != slog.LevelDebug {
		t.Errorf("level during a handshake = %v, want Debug", got)
	}
	c.endHandshake()
	if got := c.transportFaultLevel(); got != slog.LevelError {
		t.Errorf("level after the handshake = %v, want Error", got)
	}
}

// TestTransportFaultLevel_Nests is why the state is a counter and not a bool.
// ensureRoute opens a handshake region around the probe and awaitRouteActive
// opens another around each of its own attempts, so the regions overlap. With a
// flag the inner end clears the outer region and the rest of the cold start logs
// its expected faults at ERROR — the bug this replaces.
func TestTransportFaultLevel_Nests(t *testing.T) {
	c := &Client{tx: &transport{}, dropped: make(chan struct{})}

	c.beginHandshake() // outer: ensureRoute
	c.beginHandshake() // inner: one awaitRouteActive attempt
	c.endHandshake()   // inner done — the outer region is still open
	if got := c.transportFaultLevel(); got != slog.LevelDebug {
		t.Errorf("level with the outer handshake still open = %v, want Debug", got)
	}
	c.endHandshake() // outer done
	if got := c.transportFaultLevel(); got != slog.LevelError {
		t.Errorf("level after both regions closed = %v, want Error", got)
	}
}

// TestTransportFaultLevel_ClampsOverRelease: an unbalanced end must not drive
// the count negative. A negative count reads as "not handshaking" here but would
// then need two begins to demote again, so the next real cold start logs its
// expected faults at ERROR.
func TestTransportFaultLevel_ClampsOverRelease(t *testing.T) {
	c := &Client{tx: &transport{}, dropped: make(chan struct{})}

	c.endHandshake() // stray release, e.g. a double-deferred cleanup
	if got := c.handshaking.Load(); got != 0 {
		t.Errorf("handshake count after a stray release = %d, want 0", got)
	}
	c.beginHandshake()
	if got := c.transportFaultLevel(); got != slog.LevelDebug {
		t.Errorf("level after clamp+begin = %v, want Debug — the clamp did not hold", got)
	}
}

// TestHandshakeDropLogsBelowError drives the real listen path: the PLC accepts
// the socket and then resets it, which is what a PLC does for a source NetID it
// has no route for. With a handshake in flight that must not reach ERROR.
func TestHandshakeDropLogsBelowError(t *testing.T) {
	srv := startScriptableServer(t)
	defer srv.stop()

	logs := &testLogHandler{}
	c, err := Dial(srv.host, srv.port, AMSAddress{}, AMSAddress{}, time.Second,
		WithClientLogger(slog.New(logs)))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = c.Close() }()

	c.beginHandshake()
	// Answer nothing and drop the connection on the first request, the shape of
	// a PLC rejecting an unrouted NetID mid-probe.
	srv.dropConnAfter(CommandIDRead, 1)
	if _, err := c.GetSymbolVersion(t.Context()); err == nil {
		t.Fatal("probe unexpectedly succeeded against a server that drops the connection")
	}

	// Let the listen goroutine observe the EOF and log it.
	deadline := time.Now().Add(2 * time.Second)
	for logs.findByMessage("transport down") == nil && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}

	logs.mu.Lock()
	defer logs.mu.Unlock()
	for _, r := range logs.records {
		if r.Level >= slog.LevelError {
			t.Errorf("ERROR logged during a handshake: %q — one such line trips a downstream health check", r.Message)
		}
	}
}

// TestHandshakeGating_PerSite is the behavioural cover for the individual log
// sites. The source guard below can only see wording; this drives each fault for
// real and asserts the level *depends* on the handshake — the same provocation
// run twice, once inside a handshake region and once outside. The outside half
// is the mutation detector: un-gate any one of these sites and its "want Error"
// half still passes while the "want Debug" half fails, so a missing gate cannot
// hide behind a test that only ever looks at one state.
func TestHandshakeGating_PerSite(t *testing.T) {
	const clientTimeout = 300 * time.Millisecond
	stall := 3 * clientTimeout

	cases := []struct {
		name string
		arm  func(srv *scriptableServer)
		// provoke must fail; the error itself is not what is under test.
		provoke func(t *testing.T, c *Client) error
		// wantMsg is the gated log line this case reaches.
		wantMsg string
	}{
		{
			name:    "read request times out",
			arm:     func(srv *scriptableServer) { srv.delayBefore(CommandIDRead, uint32(GroupSymbolVersion), stall) },
			provoke: func(t *testing.T, c *Client) error { _, err := c.GetSymbolVersion(t.Context()); return err },
			wantMsg: "send request failed",
		},
		{
			name:    "write request times out",
			arm:     func(srv *scriptableServer) { srv.delayBefore(CommandIDWrite, 0x4020, stall) },
			provoke: func(t *testing.T, c *Client) error { return c.Write(t.Context(), 0x4020, 0, []byte{1}) },
			wantMsg: "error during send request for write",
		},
		{
			name:    "read state times out",
			arm:     func(srv *scriptableServer) { srv.delayBefore(CommandIDReadState, 0, stall) },
			provoke: func(t *testing.T, c *Client) error { _, err := c.ReadState(t.Context()); return err },
			wantMsg: "error during read state",
		},
		{
			name:    "connection dropped mid-request",
			arm:     func(srv *scriptableServer) { srv.dropConnAfter(CommandIDRead, 1) },
			provoke: func(t *testing.T, c *Client) error { _, err := c.GetSymbolVersion(t.Context()); return err },
			wantMsg: "transport down",
		},
	}

	for _, tc := range cases {
		for _, inHandshake := range []bool{true, false} {
			name := tc.name + "/outside handshake"
			wantLevel := slog.LevelError
			if inHandshake {
				name = tc.name + "/during handshake"
				wantLevel = slog.LevelDebug
			}
			t.Run(name, func(t *testing.T) {
				srv := startScriptableServer(t)
				defer srv.stop()

				logs := &testLogHandler{}
				c, err := Dial(srv.host, srv.port, AMSAddress{}, AMSAddress{}, clientTimeout,
					WithClientLogger(slog.New(logs)))
				if err != nil {
					t.Fatalf("Dial: %v", err)
				}
				defer func() { _ = c.Close() }()

				if inHandshake {
					c.beginHandshake()
				}
				tc.arm(srv)
				if err := tc.provoke(t, c); err == nil {
					t.Fatal("provocation unexpectedly succeeded")
				}

				// The listen-goroutine sites log after the call returns.
				deadline := time.Now().Add(2 * time.Second)
				for logs.findByMessage(tc.wantMsg) == nil && time.Now().Before(deadline) {
					time.Sleep(10 * time.Millisecond)
				}
				rec := logs.findByMessage(tc.wantMsg)
				if rec == nil {
					t.Fatalf("no %q log line — the provocation never reached the site under test", tc.wantMsg)
				}
				if rec.Level != wantLevel {
					t.Errorf("%q logged at %v, want %v", tc.wantMsg, rec.Level, wantLevel)
				}
			})
		}
	}
}

// TestNoUngatedTransportDownLogs is a source guard. The behavioural test above
// can only reach the paths it can provoke; this one pins the invariant across
// every site, including ones added later: a "transport down" fault must never be
// logged at a hardcoded Error level, because whether it is an error depends on
// whether a handshake is in flight.
//
// Deliberately narrow — it matches only the transport-down wording, so protocol
// and programming faults (header parse, sanity limit, binary.Write) are free to
// stay at Error, which is what they should be.
func TestNoUngatedTransportDownLogs(t *testing.T) {
	// Anything matching logger.Error(... "transport down" ...) on one line.
	ungated := regexp.MustCompile(`logger\.Error\([^)]*transport down`)
	for _, file := range []string{"client.go", "cmd_simple.go", "cmd_notification.go", "cmd_sum.go"} {
		src, err := os.ReadFile(file)
		if err != nil {
			t.Fatalf("read %s: %v", file, err)
		}
		for i, line := range strings.Split(string(src), "\n") {
			if ungated.MatchString(line) {
				// Two known exceptions, both unreachable from a handshake: a
				// malformed header and an oversized packet mean corruption, not
				// a link that went away.
				if strings.Contains(line, "header decode error") || strings.Contains(line, "sanity limit") {
					continue
				}
				t.Errorf("%s:%d logs a transport fault at a hardcoded Error level; use transportFaultLevel():\n\t%s",
					file, i+1, strings.TrimSpace(line))
			}
		}
	}
}
