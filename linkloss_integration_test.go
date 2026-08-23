//go:build integration

package ads

import (
	"context"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"
)

// linkloss_integration_test.go — link faults against a real PLC, automated.
//
// This is the cable-pull scenario without the cable: a loopback proxy in front of
// the PLC's ADS port, severed and restored by the test. The PLC itself is never
// disturbed, so unlike a power cycle its notification table, route entry and
// symbol version all survive the outage — which is precisely the case where the
// reconnect path has to delete handles that still exist rather than getting
// 0x715 from a rebooted device.
//
// Runs unattended, so it belongs in the normal integration sweep. The manual test
// still covers what a proxy cannot fake: the PLC actually restarting.

// linkLossSession builds a session that reaches the PLC only through the proxy.
//
// No WithRoute and TargetCheckOff on purpose: both would send UDP to sess.ip,
// which is the proxy. The route for this host already exists on the lab devices,
// and since the proxy runs locally the PLC still sees this machine's IP as the
// TCP source, so that route matches.
func linkLossSession(t *testing.T, p *tcpProxy, target AMSAddress) *Session {
	t.Helper()
	opts := []SessionOption{
		WithRequestTimeout(3 * time.Second),
		WithAutoReconnect(true),
		WithMaxReconnectAttempts(0), // unbounded: giving up would close the session
		WithTargetCheck(TargetCheckOff),
		WithBackoff(BackoffConfig{
			InitialInterval: 500 * time.Millisecond,
			InitialAttempts: 6,
			MidInterval:     time.Second,
			MidAttempts:     6,
			SlowInterval:    2 * time.Second,
			SlowAttempts:    6,
			MaxInterval:     3 * time.Second,
		}),
	}
	if localAMS := os.Getenv("ADS_LOCAL_AMS"); localAMS != "" {
		local, err := NewAMSAddress(localAMS, 10600)
		if err != nil {
			t.Fatalf("ADS_LOCAL_AMS %q: %v", localAMS, err)
		}
		opts = append(opts, WithLocalAMS(local))
	}

	sess, err := NewSession(context.Background(),
		AMSEndpoint{IP: p.host(), Port: p.port(), AMS: target}, opts...)
	if err != nil {
		t.Fatalf("NewSession via %s: %v", p, err)
	}
	t.Cleanup(func() { sess.Close() })
	return sess
}

func linkLossTarget(t *testing.T) (host string, target AMSAddress) {
	t.Helper()
	host = getEnvOrDefault("ADS_PLC_IP", "192.168.3.70")
	targetAMS := getEnvOrDefault("ADS_TARGET_AMS", "5.3.69.134.1.1")
	portStr := getEnvOrDefault("ADS_TARGET_PORT", "801")
	port, err := strconv.Atoi(portStr)
	if err != nil {
		t.Fatalf("ADS_TARGET_PORT %q: %v", portStr, err)
	}
	target, err = NewAMSAddress(targetAMS, uint16(port))
	if err != nil {
		t.Fatalf("target AMS: %v", err)
	}
	return host, target
}

// subscribeLinkLossSymbols subscribes whatever changing symbols the env offers and
// returns their count.
func subscribeLinkLossSymbols(t *testing.T, sess *Session, ch chan *Update) int {
	t.Helper()
	counter := os.Getenv("ADS_READ_COUNTER")
	if counter == "" {
		t.Skip("ADS_READ_COUNTER not set — the test needs a symbol that changes on its own")
	}
	names := []string{counter}
	for _, key := range []string{"ADS_READ_REAL", "ADS_READ_STRING"} {
		if v := os.Getenv(key); v != "" && v != counter {
			names = append(names, v)
		}
	}
	configs := make([]NotificationConfig, 0, len(names))
	for _, n := range names {
		configs = append(configs, NotificationConfig{
			SymbolName:       n,
			TransmissionMode: TransModeServerOnChange,
			MaxDelay:         200 * time.Millisecond,
			CycleTime:        200 * time.Millisecond,
		})
	}
	results, err := sess.AddSymbolNotifications(context.Background(), configs, ch)
	if err != nil {
		t.Fatalf("AddSymbolNotifications: %v", err)
	}
	bound := 0
	for i, r := range results {
		if r.Skipped != nil || r.Handle == 0 {
			t.Logf("note: %s not subscribed (%v)", names[i], r.Skipped)
			continue
		}
		bound++
	}
	if bound == 0 {
		t.Fatal("no symbol subscribed; nothing to observe")
	}
	return bound
}

func drainUpdates(ch <-chan *Update, d time.Duration) int {
	n := 0
	deadline := time.After(d)
	for {
		select {
		case <-ch:
			n++
		case <-deadline:
			return n
		}
	}
}

func awaitQuiet(ch <-chan *Update, quiet, limit time.Duration) bool {
	deadline := time.After(limit)
	for {
		select {
		case <-ch:
		case <-time.After(quiet):
			return true
		case <-deadline:
			return false
		}
	}
}

func awaitUpdate(ch <-chan *Update, limit time.Duration) bool {
	select {
	case <-ch:
		return true
	case <-time.After(limit):
		return false
	}
}

// TestIntegrationLinkLossBlackhole is the pulled-cable shape: the wire goes quiet
// with the sockets still open, so requests time out rather than being refused,
// and a redial hangs instead of failing fast. The PLC keeps everything — its
// notification table still holds our handles when we come back inside the
// route-idle timeout.
func TestIntegrationLinkLossBlackhole(t *testing.T) {
	host, target := linkLossTarget(t)
	p := startTCPProxy(t, net.JoinHostPort(host, "48898"))
	sess := linkLossSession(t, p, target)

	ctx := context.Background()
	if err := sess.Connect(ctx); err != nil {
		t.Fatalf("Connect through %s: %v", p, err)
	}
	if err := sess.LoadSymbols(ctx); err != nil {
		t.Fatalf("LoadSymbols: %v", err)
	}

	ch := make(chan *Update, 256)
	bound := subscribeLinkLossSymbols(t, sess, ch)
	if n := drainUpdates(ch, 5*time.Second); n == 0 {
		t.Fatal("no updates before the outage — fix the setup before testing recovery")
	}
	t.Logf("streaming %d symbols through %s", bound, p)

	// Sever.
	p.blackhole()
	if !awaitQuiet(ch, 3*time.Second, 30*time.Second) {
		t.Fatal("updates never stopped after blackholing the link")
	}
	downAt := time.Now()
	t.Logf("stream stopped %v after the link went dark", time.Since(downAt).Round(time.Millisecond))

	// Stay down long enough for the library to notice and start retrying, but
	// well inside the PLC's route-idle timeout so its handle table still holds
	// our registrations when we return.
	time.Sleep(10 * time.Second)
	if sess.IsClosed() {
		t.Fatal("session closed itself during a transient outage; unbounded retries must keep it alive")
	}

	// Restore.
	p.restore()
	if !awaitUpdate(ch, 90*time.Second) {
		state := sess.lifecycle.state.load()
		t.Fatalf("no updates within 90s of the link returning; state=%v IsClosed=%v", state, sess.IsClosed())
	}
	t.Logf("recovered %v after the outage began", time.Since(downAt).Round(time.Second))

	assertHealthyAfterRecovery(t, sess, ch, bound)
	accepted, toPLC, toClient := p.stats()
	t.Logf("proxy: %d connections accepted, %d bytes to PLC, %d bytes to client", accepted, toPLC, toClient)
	if accepted < 2 {
		t.Errorf("proxy accepted %d connections; a recovery must have dialed again", accepted)
	}
}

// TestIntegrationLinkLossCut is the other shape: the port drops and live
// connections die immediately, so the client sees EOF now instead of a timeout
// later. Same recovery contract, different detection path — listen() observing a
// closed socket rather than a request deadline.
func TestIntegrationLinkLossCut(t *testing.T) {
	host, target := linkLossTarget(t)
	p := startTCPProxy(t, net.JoinHostPort(host, "48898"))
	sess := linkLossSession(t, p, target)

	ctx := context.Background()
	if err := sess.Connect(ctx); err != nil {
		t.Fatalf("Connect through %s: %v", p, err)
	}
	if err := sess.LoadSymbols(ctx); err != nil {
		t.Fatalf("LoadSymbols: %v", err)
	}

	ch := make(chan *Update, 256)
	bound := subscribeLinkLossSymbols(t, sess, ch)
	if n := drainUpdates(ch, 5*time.Second); n == 0 {
		t.Fatal("no updates before the outage")
	}

	p.cut()
	if !awaitQuiet(ch, 2*time.Second, 30*time.Second) {
		t.Fatal("updates never stopped after cutting the link")
	}
	downAt := time.Now()

	time.Sleep(5 * time.Second)
	p.restore()

	if !awaitUpdate(ch, 90*time.Second) {
		t.Fatalf("no updates within 90s of the link returning; state=%v IsClosed=%v",
			sess.lifecycle.state.load(), sess.IsClosed())
	}
	t.Logf("recovered %v after the cut", time.Since(downAt).Round(time.Second))
	assertHealthyAfterRecovery(t, sess, ch, bound)
}

// TestIntegrationLinkLossFlapping repeats a short outage several times. Each cycle
// leaves handles behind on a PLC that never restarted, so this is the case that
// accumulates entries in the notification table if the reconnect path fails to
// release them (Beckhoff #268). It also drives the cross-cycle flap cooldown.
func TestIntegrationLinkLossFlapping(t *testing.T) {
	host, target := linkLossTarget(t)
	p := startTCPProxy(t, net.JoinHostPort(host, "48898"))
	sess := linkLossSession(t, p, target)

	ctx := context.Background()
	if err := sess.Connect(ctx); err != nil {
		t.Fatalf("Connect through %s: %v", p, err)
	}
	if err := sess.LoadSymbols(ctx); err != nil {
		t.Fatalf("LoadSymbols: %v", err)
	}

	ch := make(chan *Update, 512)
	bound := subscribeLinkLossSymbols(t, sess, ch)
	if n := drainUpdates(ch, 5*time.Second); n == 0 {
		t.Fatal("no updates before the first outage")
	}

	const cycles = 3
	for i := 1; i <= cycles; i++ {
		p.cut()
		if !awaitQuiet(ch, 2*time.Second, 30*time.Second) {
			t.Fatalf("cycle %d: updates never stopped", i)
		}
		time.Sleep(2 * time.Second)
		p.restore()
		if !awaitUpdate(ch, 90*time.Second) {
			t.Fatalf("cycle %d: no recovery within 90s; state=%v IsClosed=%v",
				i, sess.lifecycle.state.load(), sess.IsClosed())
		}
		t.Logf("cycle %d recovered", i)
	}

	assertHealthyAfterRecovery(t, sess, ch, bound)
	sess.notifications.lock.Lock()
	active := len(sess.notifications.activeNotifications)
	pending := len(sess.notifications.pending)
	sess.notifications.lock.Unlock()
	// One entry per subscribed symbol, no matter how many cycles ran: a config
	// list that grows per flap is the resubscribe path duplicating work.
	if active != bound || pending != bound {
		t.Errorf("after %d flaps: active=%d pending=%d, want %d each", cycles, active, pending, bound)
	}
}

// assertHealthyAfterRecovery checks the session is genuinely usable again, not
// merely emitting samples: right state, every symbol re-bound and streaming, and
// the request path working.
func assertHealthyAfterRecovery(t *testing.T, sess *Session, ch <-chan *Update, want int) {
	t.Helper()
	if sess.IsClosed() {
		t.Error("session reports closed although updates resumed")
	}
	if state := sess.lifecycle.state.load(); state != SessionStateConnected {
		t.Errorf("state = %v, want Connected", state)
	}

	sess.notifications.lock.Lock()
	bound := len(sess.notifications.activeNotifications)
	sess.notifications.lock.Unlock()
	if bound != want {
		t.Errorf("bound notifications = %d, want %d — re-subscribe did not restore every symbol", bound, want)
	}

	seen := map[string]bool{}
	deadline := time.After(60 * time.Second)
	for len(seen) < want {
		select {
		case u := <-ch:
			seen[strings.ToLower(u.Variable)] = true
		case <-deadline:
			t.Errorf("only %d/%d symbols streamed after recovery: %v", len(seen), want, keysOf(seen))
			return
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if name := os.Getenv("ADS_READ_COUNTER"); name != "" {
		if _, err := sess.ReadFromSymbol(ctx, name); err != nil {
			t.Errorf("ReadFromSymbol after recovery: %v", err)
		}
	}
}

func keysOf(m map[string]bool) string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return fmt.Sprint(out)
}
