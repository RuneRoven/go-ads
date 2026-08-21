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
	c.setHandshaking(true)
	if got := c.transportFaultLevel(); got != slog.LevelDebug {
		t.Errorf("level during a handshake = %v, want Debug", got)
	}
	c.setHandshaking(false)
	if got := c.transportFaultLevel(); got != slog.LevelError {
		t.Errorf("level after the handshake = %v, want Error", got)
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

	c.setHandshaking(true)
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
