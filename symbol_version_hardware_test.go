//go:build symbol_version_hardware

// Symbol-version hardware tests — online-change strategies.
//
// REQUIREMENTS (Plan-C, R-CACHE-009..014, R-NOT-016/017, R-SES-011):
//   - PLC project must contain MAIN_DP1 with: nCounter (DINT), sStable (STRING),
//     nProbeA (INT), nProbeB (DINT), stStruct (ST_DP1)
//   - DUT ST_DP1 with members: a (INT), b (INT, removable), c (LREAL, movable)
//
// Run pattern (manual gate, ~3 cycles per strategy):
//   go test -tags symbol_version_hardware -v -timeout 600s -run TestSymbolVersion .
//
// Each subtest prompts for online-change activation in TwinCAT XAE.
// Set SYMVER_AUTOMATED=1 to bypass prompts (CI mode — out of scope until XAE remote
// trigger is wired).
//
// Validates: R-CACHE-009, R-CACHE-010, R-CACHE-011, R-CACHE-012, R-CACHE-013,
//            R-NOT-016, R-NOT-017, R-SES-011.

package ads

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"
)

const (
	symCounter = "MAIN_DP1.nCounter"
	symStable  = "MAIN_DP1.sStable"
	symProbeA  = "MAIN_DP1.nProbeA"
	symProbeB  = "MAIN_DP1.nProbeB"
	symStructC = "MAIN_DP1.stStruct.c"
)

// symVerEnv returns the env var or fallback. Local copy so this file builds
// independent of the `integration` build tag (which gates getEnvOrDefault).
func symVerEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// waitForOperator blocks until the operator confirms the online-change
// activation in TwinCAT XAE. Skipped under SYMVER_AUTOMATED=1.
//
// Reads from /dev/tty directly: `go test` redirects os.Stdin (immediate EOF)
// and buffers os.Stdout (prompt invisible until test ends). /dev/tty is the
// controlling terminal — bypasses both.
func waitForOperator(t *testing.T, prompt string) {
	t.Helper()
	if os.Getenv("SYMVER_AUTOMATED") == "1" {
		t.Logf("SYMVER_AUTOMATED=1 — skipping operator gate (%s)", prompt)
		time.Sleep(5 * time.Second) // allow asynchronous activation
		return
	}
	tty, err := os.OpenFile("/dev/tty", os.O_RDWR, 0)
	if err != nil {
		t.Skipf("manual gate needs controlling terminal (/dev/tty): %v", err)
	}
	defer tty.Close()
	fmt.Fprintf(tty, "\n>>> ACTIVATE ONLINE CHANGE NOW: %s\n>>> press ENTER when done <<<\n", prompt)
	if _, err := bufio.NewReader(tty).ReadBytes('\n'); err != nil {
		t.Fatalf("operator gate read: %v", err)
	}
}

// symbolVersionSession builds a Session against TC3 (the symbol-version hardware target) with
// the strategy parameter applied via online-change strategy options applied once R-SES-011 is implemented.
//
// strategy: "auto" | "close" | "ignore"
func symbolVersionSession(t *testing.T, _strategy string) *Session {
	t.Helper()
	ip := symVerEnv("ADS_PLC_IP", "192.168.3.224")
	targetAMS := symVerEnv("ADS_TARGET_AMS", "5.154.236.19.1.1")
	targetPortStr := symVerEnv("ADS_TARGET_PORT", "851")
	targetPort, err := strconv.Atoi(targetPortStr)
	if err != nil {
		t.Fatalf("invalid ADS_TARGET_PORT %q: %v", targetPortStr, err)
	}
	localAMS := symVerEnv("ADS_LOCAL_AMS", "auto")

	// TODO(R-SES-011): once R-SES-011 lands, append:
	//   var opts []SessionOption
	//   switch _strategy {
	//   case "auto":   opts = append(opts, WithSymbolVersionStrategy(SymbolVersionAutoReload))
	//   case "close":  opts = append(opts, WithSymbolVersionStrategy(SymbolVersionClose))
	//   case "ignore": opts = append(opts, WithSymbolVersionStrategy(SymbolVersionIgnore))
	//   }
	//   opts = append(opts, WithOnSymbolVersionChanged(func(reason string) { t.Logf("symbol-version-changed cb: %s", reason) }))
	//   pass opts into NewSession (signature change required) or apply via setter.

	sess, err := NewSession(ip, 48898, targetAMS, targetPort, localAMS, 11000, 5*time.Second)
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	if err := sess.Connect(false); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	t.Cleanup(func() { sess.Close() })

	if err := sess.LoadSymbols(); err != nil {
		t.Fatalf("LoadSymbols: %v", err)
	}
	return sess
}

// assertSymbolPresent fails the test fast if PLC project lacks the online-change test POU.
func assertSymbolPresent(t *testing.T, sess *Session, name string) {
	t.Helper()
	syms, _ := sess.ListSymbols()
	if _, ok := syms[name]; !ok {
		t.Skipf("PLC missing %q — load MAIN_DP1 + ST_DP1 into TC3 project first", name)
	}
}

// drainUpdates collects up to n updates or returns whatever arrived within
// timeout, whichever first.
func drainUpdates(ch <-chan *Update, n int, timeout time.Duration) []*Update {
	var out []*Update
	deadline := time.After(timeout)
	for len(out) < n {
		select {
		case u, ok := <-ch:
			if !ok {
				return out
			}
			out = append(out, u)
		case <-deadline:
			return out
		}
	}
	return out
}

// sampleCollector wraps a notification channel with a background goroutine that
// keeps the channel drained while still recording samples for assertion.
// Required for high-frequency PLC symbols (e.g. nCounter at 100Hz) where
// synchronous drainUpdates can't keep up with the scan cycle and trips the
// "channel full" warn path.
type sampleCollector struct {
	mu      sync.Mutex
	samples []*Update
	stop    chan struct{}
	done    chan struct{}
}

func startCollector(ch <-chan *Update) *sampleCollector {
	c := &sampleCollector{
		stop: make(chan struct{}),
		done: make(chan struct{}),
	}
	go func() {
		defer close(c.done)
		for {
			select {
			case <-c.stop:
				// Drain residual without blocking.
				for {
					select {
					case u, ok := <-ch:
						if !ok {
							return
						}
						c.mu.Lock()
						c.samples = append(c.samples, u)
						c.mu.Unlock()
					default:
						return
					}
				}
			case u, ok := <-ch:
				if !ok {
					return
				}
				c.mu.Lock()
				c.samples = append(c.samples, u)
				c.mu.Unlock()
			}
		}
	}()
	return c
}

// snapshot returns a copy of samples collected so far.
func (c *sampleCollector) snapshot() []*Update {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]*Update, len(c.samples))
	copy(out, c.samples)
	return out
}

// reset clears collected samples (used to mark pre/post boundaries).
func (c *sampleCollector) reset() {
	c.mu.Lock()
	c.samples = nil
	c.mu.Unlock()
}

// stopAndWait halts the goroutine.
func (c *sampleCollector) stopAndWait() {
	close(c.stop)
	<-c.done
}

// =============================================================================
// Strategy: SymbolVersionAutoReload (default)
// =============================================================================

// TestSymbolVersionAutoReload_TypeChange — cycle 1: nProbeA INT → LREAL.
//
// Expected: detection bumps SymbolVersion → reload completes → resubscribe
// reissues handle → fresh sample arrives with Stale=false. Callback fires
// once with reason="symbol-version-invalid".
//
// Validates: R-CACHE-010, R-NOT-017 (AutoReload branch).
func TestSymbolVersionAutoReload_TypeChange(t *testing.T) {
	sess := symbolVersionSession(t, "auto")
	assertSymbolPresent(t, sess, symProbeA)

	// ServerCycle (period sample) — fires every 100ms regardless of value
	// change. Required because nProbeA may not be written to by PLC code,
	// so ServerOnChange would yield 0 samples.
	ch := make(chan *Update, 256)
	if _, err := sess.AddSymbolNotification(symProbeA, 100*time.Millisecond, 100*time.Millisecond, TransModeServerCycle, ch); err != nil {
		t.Fatalf("AddSymbolNotification: %v", err)
	}
	col := startCollector(ch)
	defer col.stopAndWait()

	time.Sleep(2 * time.Second)
	pre := col.snapshot()
	if len(pre) == 0 {
		t.Fatal("no pre-change samples — check ServerCycle support on PLC")
	}
	t.Logf("pre-change samples: %d (over 2s)", len(pre))
	col.reset()

	waitForOperator(t, "TOGGLE MAIN_DP1.nProbeA type (INT↔LREAL), then Activate")

	time.Sleep(5 * time.Second)
	post := col.snapshot()
	if len(post) == 0 {
		t.Fatal("no post-change samples — AutoReload reload+resub failed")
	}
	// TODO(R-SES-011): assert post[0].Stale=true once R-NOT-016 lands.
	t.Logf("post-change samples: %d, first value=%v", len(post), post[0].Value)
}

// TestSymbolVersionAutoReload_SymbolRemoved — cycle 2: delete nProbeB.
//
// Expected: 0x720 path triggers callback reason="symbol-not-found"; reload
// completes minus B; subsequent reads on B return ErrSymbolNotFound.
//
// Validates: R-CACHE-010, R-NOT-017.
func TestSymbolVersionAutoReload_SymbolRemoved(t *testing.T) {
	sess := symbolVersionSession(t, "auto")
	assertSymbolPresent(t, sess, symProbeB)

	ch := make(chan *Update, 16)
	if _, err := sess.AddSymbolNotification(symProbeB, 100*time.Millisecond, 100*time.Millisecond, TransModeServerOnChange, ch); err != nil {
		t.Fatalf("AddSymbolNotification: %v", err)
	}

	// Confirm baseline read works.
	if _, err := sess.ReadFromSymbol(symProbeB); err != nil {
		t.Fatalf("baseline read: %v", err)
	}

	waitForOperator(t, "delete MAIN_DP1.nProbeB declaration, then Activate")

	// Post-change: read should fail; channel may receive Stale terminal sample.
	time.Sleep(3 * time.Second) // let auto-reload settle
	if _, err := sess.ReadFromSymbol(symProbeB); err == nil {
		t.Error("expected read error after symbol removal, got nil")
	} else {
		t.Logf("post-removal read err (expected): %v", err)
	}
}

// TestSymbolVersionAutoReload_StructMemberOffsetShift — cycle 3: drop ST_DP1.b shifts c.
//
// Expected: stStruct.c offset shifts; reload re-resolves; notifications continue
// against new offset.
//
// Validates: R-CACHE-010 (struct re-resolution path).
func TestSymbolVersionAutoReload_StructMemberOffsetShift(t *testing.T) {
	sess := symbolVersionSession(t, "auto")
	assertSymbolPresent(t, sess, symStructC)

	ch := make(chan *Update, 256)
	if _, err := sess.AddSymbolNotification(symStructC, 100*time.Millisecond, 100*time.Millisecond, TransModeServerCycle, ch); err != nil {
		t.Fatalf("AddSymbolNotification: %v", err)
	}
	col := startCollector(ch)
	defer col.stopAndWait()

	time.Sleep(2 * time.Second)
	pre := col.snapshot()
	t.Logf("pre samples: %d (over 2s)", len(pre))
	col.reset()

	waitForOperator(t, "delete ST_DP1.b member OR add new member to ST_DP1 (shifts c offset), then Activate")

	time.Sleep(5 * time.Second)
	post := col.snapshot()
	if len(post) == 0 {
		t.Fatal("no post-change samples — struct re-resolution failed")
	}
	t.Logf("post samples: %d, first value=%v", len(post), post[0].Value)
}

// TestSymbolVersionAutoReload_CapExhaustion — repeated rapid changes exceed
// MaxSymbolVersionReloadAttempts (default 3 / 60s window). Library SHALL
// degrade to Ignore semantics and emit WARN log + onSymbolVersionChanged
// callback per detection.
//
// Validates: R-CACHE-013.
func TestSymbolVersionAutoReload_CapExhaustion(t *testing.T) {
	t.Skip("manual: requires 4 rapid online-change cycles within 60s — operator-fatigue test")
	// Operator script: rename + revert MAIN_DP1.nCounter four times in <60s.
	// Assertion: 4th detection logs WARN "reload-cap-exhausted" and sets
	// Update.Reason="reload-cap-exhausted" on the next sample.
}

// =============================================================================
// Strategy: SymbolVersionClose
// =============================================================================

// TestSymbolVersionClose_OnDetection — any 0x711/0x720/0x745 closes session immediately.
//
// Expected: onDisconnect fires, notification channel closes, subsequent ops
// return ErrDisconnected.
//
// Validates: R-CACHE-011, R-NOT-017 (Close branch).
func TestSymbolVersionClose_OnDetection(t *testing.T) {
	sess := symbolVersionSession(t, "close")
	assertSymbolPresent(t, sess, symProbeA)

	closed := make(chan struct{})
	// TODO(R-SES-011): wire sess.OnDisconnect(func() { close(closed) }) once exposed.
	_ = closed

	ch := make(chan *Update, 256)
	if _, err := sess.AddSymbolNotification(symProbeA, 100*time.Millisecond, 100*time.Millisecond, TransModeServerCycle, ch); err != nil {
		t.Fatalf("AddSymbolNotification: %v", err)
	}
	col := startCollector(ch)
	defer col.stopAndWait()

	waitForOperator(t, "TOGGLE MAIN_DP1.nProbeA type (INT↔LREAL), then Activate")

	// HARDWARE FINDING: TC3 keeps firing notifications through the OLD handle
	// after online change. Detection only triggers via explicit op (Read).
	// Probe with a Read; under "close" strategy the resulting 0x711/0x0703
	// must close the session.
	_, readErr := sess.ReadFromSymbol(symProbeA)
	t.Logf("post-change probe read err: %v", readErr)

	// TODO(R-SES-011): Once Close strategy lands, assert:
	//   - sess.IsClosed() == true within 2s
	//   - subsequent notification samples drain + channel closes
	//   - onDisconnect callback fired exactly once
	time.Sleep(2 * time.Second)
	post := col.snapshot()
	t.Logf("post-probe samples in collector: %d (expect 0 after channel close)", len(post))
}

// =============================================================================
// Strategy: SymbolVersionIgnore
// =============================================================================

// TestSymbolVersionIgnore_StaleFlag — stale cache used; first post-change sample marked
// Stale=true Reason="symbol-version-invalid".
//
// Expected: dispatch continues using OLD handle (PLC may still honor it for
// unchanged symbols); first sample marked stale; subsequent samples Stale=false.
//
// Validates: R-CACHE-012, R-NOT-016, R-NOT-017 (Ignore branch).
func TestSymbolVersionIgnore_StaleFlag(t *testing.T) {
	sess := symbolVersionSession(t, "ignore")
	assertSymbolPresent(t, sess, symCounter) // counter SURVIVES online change

	// Counter increments every PLC scan (10ms = 100Hz). Use big buffer +
	// active drainer goroutine so the channel never blocks the listen loop.
	ch := make(chan *Update, 1024)
	if _, err := sess.AddSymbolNotification(symCounter, 100*time.Millisecond, 100*time.Millisecond, TransModeServerOnChange, ch); err != nil {
		t.Fatalf("AddSymbolNotification: %v", err)
	}
	col := startCollector(ch)
	defer col.stopAndWait()

	time.Sleep(2 * time.Second) // baseline window
	pre := col.snapshot()
	if len(pre) == 0 {
		t.Fatal("no pre samples")
	}
	t.Logf("pre samples: %d (over 2s)", len(pre))
	col.reset()

	waitForOperator(t, "TOGGLE MAIN_DP1.nProbeA type (INT↔LREAL) — any change bumps SymbolVersion, then Activate")

	time.Sleep(3 * time.Second) // post-change observation window
	post := col.snapshot()
	if len(post) == 0 {
		t.Fatal("no post samples under Ignore — OLD handle should still flow")
	}
	// TODO(R-SES-011): once R-NOT-016 lands, assert:
	//   if !post[0].Stale || post[0].Reason != "symbol-version-invalid" {
	//       t.Errorf("first post-change sample: Stale=%v Reason=%q, want Stale=true Reason=symbol-version-invalid",
	//           post[0].Stale, post[0].Reason)
	//   }
	t.Logf("post samples: %d, first value=%v", len(post), post[0].Value)
}

// TestSymbolVersionIgnore_RemovedSymbolStops — when symbol B is deleted, OLD handle
// stops yielding samples but no error is raised (Ignore strategy).
//
// Validates: R-CACHE-012.
func TestSymbolVersionIgnore_RemovedSymbolStops(t *testing.T) {
	sess := symbolVersionSession(t, "ignore")
	assertSymbolPresent(t, sess, symProbeB)

	// ServerCycle so we get steady-state samples pre-deletion (proves
	// subscription alive); silence post-deletion is the assertion target.
	ch := make(chan *Update, 256)
	if _, err := sess.AddSymbolNotification(symProbeB, 100*time.Millisecond, 100*time.Millisecond, TransModeServerCycle, ch); err != nil {
		t.Fatalf("AddSymbolNotification: %v", err)
	}
	col := startCollector(ch)
	defer col.stopAndWait()

	time.Sleep(2 * time.Second)
	pre := col.snapshot()
	if len(pre) == 0 {
		t.Fatal("no pre samples — ServerCycle subscription not flowing")
	}
	t.Logf("pre samples: %d", len(pre))
	col.reset()

	waitForOperator(t, "delete MAIN_DP1.nProbeB declaration, then Activate")

	time.Sleep(3 * time.Second)
	post := col.snapshot()
	t.Logf("post-removal Ignore samples: %d (expect 0-1 stale terminal)", len(post))
	for i, u := range post {
		t.Logf("  [%d] value=%v", i, u.Value)
	}
	// TODO(R-SES-011): once R-NOT-016 lands, assert .Stale=true on terminal sample.
}
