//go:build integration

package ads

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
)

// manual_reconnect_integration_test.go — recovery against real hardware, with a
// human doing the disruption.
//
// Everything else in the suite fakes the disruption: a stub drops a socket, a
// test cancels a context. None of that reproduces what the PLC actually does when
// it reboots (route table possibly empty, handles gone, symbol version changed),
// when its runtime restarts (transport survives, symbols change underneath), or
// when the cable is pulled (transport dies, PLC state untouched). Those three
// differ in which recovery path runs, so they are three separate runs.
//
// Gated on ADS_MANUAL_RESTART so an unattended suite never blocks. No keypress is
// needed: the test watches the update stream, so it detects the disruption and the
// recovery on its own. Run it, do the thing, watch the timeline.
//
//	set -a; . ./.env.integration.70; set +a
//	ADS_MANUAL_RESTART=powercycle go test -tags integration -v \
//	    -timeout 20m -run TestManualRestartRecovery .
//
// ADS_MANUAL_RESTART selects the scenario and only changes what it prints and how
// long it waits — the assertions are the same, because the library's promise is
// the same in all three cases.

// Defaults are sized for a human plus a booting PLC. Each is overridable so the
// harness itself can be smoke-tested in seconds (ADS_MANUAL_PROOF=2s
// ADS_MANUAL_DISRUPT_WAIT=10s ADS_MANUAL_RECOVERY=20s) without editing the file.
var (
	manualStreamProof   = manualDur("ADS_MANUAL_PROOF", 20*time.Second)       // updates must flow before we disturb anything
	manualDisruptWait   = manualDur("ADS_MANUAL_DISRUPT_WAIT", 5*time.Minute) // how long to wait for the human
	manualRecoveryGrace = manualDur("ADS_MANUAL_RECOVERY", 4*time.Minute)     // PLC boot + route + reload + resubscribe
)

func manualDur(env string, def time.Duration) time.Duration {
	if v := os.Getenv(env); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			return d
		}
	}
	return def
}

func manualInt(env string, def int) int {
	if v := os.Getenv(env); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return def
}

// manualSubscriptionCount is how many symbols the manual scenarios subscribe.
//
// Three proved too little: a real consumer subscribes tens of tags, and the paths
// that matter at that size are different ones. Forty puts the re-subscribe through
// the sum command as a genuine batch, gives the per-config retry accounting
// something to get wrong, and makes a partial recovery visible — with three
// symbols, "all of them came back" and "the batch was retried correctly" are the
// same observation.
var manualSubscriptionCount = manualInt("ADS_MANUAL_SYMBOLS", 40)

// manualFillerTypes are the data types worth subscribing in bulk: primitives whose
// samples are small and whose decode is exercised elsewhere. Structs and arrays are
// skipped so a slow PLC is not asked to push kilobytes per cycle per handle.
var manualFillerTypes = map[string]bool{
	"BOOL": true, "BYTE": true, "SINT": true, "USINT": true,
	"INT": true, "UINT": true, "WORD": true,
	"DINT": true, "UDINT": true, "DWORD": true,
	"LINT": true, "ULINT": true, "LWORD": true,
	"REAL": true, "LREAL": true,
	"TIME": true, "DATE": true, "DT": true, "TOD": true,
}

// fillerSymbols picks additional symbols to subscribe, in a stable order so two
// runs against the same PLC subscribe the same set.
//
// They go on ServerCycle rather than ServerOnChange: the recovery phase requires
// every subscribed symbol to deliver again, and a constant symbol on-change is
// legitimately silent forever, which would make this test fail for a reason that
// is not a defect. The env-named symbols stay on-change, so both modes are covered.
func fillerSymbols(sess *Session, exclude map[string]bool, want int) []string {
	sess.cache.lock.Lock()
	candidates := make([]string, 0, len(sess.cache.symbols))
	for _, sym := range sess.cache.symbols {
		if sym == nil || exclude[strings.ToLower(sym.FullName)] {
			continue
		}
		if !manualFillerTypes[strings.ToUpper(sym.DataType)] {
			continue
		}
		if strings.Contains(sym.FullName, "[") {
			continue // array element
		}
		candidates = append(candidates, sym.FullName)
	}
	sess.cache.lock.Unlock()
	sort.Strings(candidates)
	if len(candidates) > want {
		candidates = candidates[:want]
	}
	return candidates
}

func TestManualRestartRecovery(t *testing.T) {
	scenario := os.Getenv("ADS_MANUAL_RESTART")
	if scenario == "" {
		t.Skip("ADS_MANUAL_RESTART not set (powercycle | runtime | cable) — this test needs a human")
	}

	var instruction string
	switch strings.ToLower(scenario) {
	case "powercycle":
		instruction = "POWER-CYCLE the PLC now (pull power, wait for it to boot back up)"
	case "runtime":
		instruction = "RESTART THE TWINCAT RUNTIME now (or activate the configuration) — leave power and network alone"
	case "cable":
		instruction = "UNPLUG THE NETWORK CABLE now, wait ~15s, then PLUG IT BACK IN"
	default:
		t.Fatalf("ADS_MANUAL_RESTART=%q — want powercycle, runtime or cable", scenario)
	}

	host := getEnvOrDefault("ADS_PLC_IP", "192.168.3.70")
	symbol := os.Getenv("ADS_READ_COUNTER")
	if symbol == "" {
		t.Skip("ADS_READ_COUNTER not set — the test needs a symbol that changes on its own")
	}
	targetAMS := getEnvOrDefault("ADS_TARGET_AMS", "5.3.69.134.1.1")
	portStr := getEnvOrDefault("ADS_TARGET_PORT", "801")
	port, err := strconv.Atoi(portStr)
	if err != nil {
		t.Fatalf("ADS_TARGET_PORT %q: %v", portStr, err)
	}
	target, err := NewAMSAddress(targetAMS, uint16(port))
	if err != nil {
		t.Fatalf("target AMS: %v", err)
	}

	opts := []SessionOption{
		WithRequestTimeout(5 * time.Second),
		WithAutoReconnect(true),
		// Unbounded: giving up would close the session, and a PLC that takes two
		// minutes to boot is not a failure.
		WithMaxReconnectAttempts(0),
		WithSymbolVersionStrategy(SymbolVersionAutoReload),
	}
	if hostIP := os.Getenv("ADS_HOST_IP"); hostIP != "" {
		opts = append(opts, WithHostIP(hostIP))
	}
	if u, p := os.Getenv("ADS_ROUTE_USER"), os.Getenv("ADS_ROUTE_PASS"); u != "" && p != "" {
		opts = append(opts, WithRoute("go-ads-manual-restart", u, p))
	}
	if localAMS := os.Getenv("ADS_LOCAL_AMS"); localAMS != "" {
		local, err := NewAMSAddress(localAMS, 10500)
		if err != nil {
			t.Fatalf("ADS_LOCAL_AMS %q: %v", localAMS, err)
		}
		opts = append(opts, WithLocalAMS(local))
	}
	if bindIP := os.Getenv("ADS_LOCAL_BIND_IP"); bindIP != "" {
		opts = append(opts, WithLocalBindIP(bindIP))
	}

	ctx := context.Background()
	sess, err := NewSession(ctx, AMSEndpoint{IP: host, AMS: target}, opts...)
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	defer sess.Close()
	if err := sess.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	if err := sess.LoadSymbols(ctx); err != nil {
		t.Fatalf("LoadSymbols: %v", err)
	}

	// Subscribe the env-named symbols on change, then fill up to
	// manualSubscriptionCount from the symbol table on cycle. Re-subscribe has to
	// restore all of them, not just the one being watched for liveness.
	names := []string{symbol}
	for _, key := range []string{"ADS_READ_REAL", "ADS_READ_STRING"} {
		if v := os.Getenv(key); v != "" && v != symbol {
			names = append(names, v)
		}
	}
	onChange := len(names)
	taken := make(map[string]bool, len(names))
	for _, n := range names {
		taken[strings.ToLower(n)] = true
	}
	if extra := manualSubscriptionCount - len(names); extra > 0 {
		filler := fillerSymbols(sess, taken, extra)
		names = append(names, filler...)
	}
	if len(names) < manualSubscriptionCount {
		t.Logf("note: only %d subscribable symbols found, wanted %d", len(names), manualSubscriptionCount)
	}
	configs := make([]NotificationConfig, 0, len(names))
	for i, n := range names {
		mode := TransModeServerCycle
		if i < onChange {
			mode = TransModeServerOnChange
		}
		configs = append(configs, NotificationConfig{
			SymbolName:       n,
			TransmissionMode: mode,
			MaxDelay:         200 * time.Millisecond,
			CycleTime:        200 * time.Millisecond,
		})
	}
	ch := make(chan *Update, 256)
	results, err := sess.AddSymbolNotifications(ctx, configs, ch)
	if err != nil {
		t.Fatalf("AddSymbolNotifications: %v", err)
	}
	subscribed := 0
	for i, r := range results {
		if r.Skipped != nil || r.Handle == 0 {
			t.Logf("note: %s not subscribed (%v)", names[i], r.Skipped)
			continue
		}
		subscribed++
	}
	if subscribed == 0 {
		t.Fatal("no symbol subscribed; nothing to observe")
	}
	t.Logf("subscribed %d/%d symbols on %s", subscribed, len(configs), host)

	timeline := func(format string, args ...any) {
		t.Logf("[%s] %s", time.Now().Format("15:04:05"), fmt.Sprintf(format, args...))
	}

	// Phase 1 — prove the stream is live before disturbing anything.
	before := drainFor(ch, manualStreamProof)
	if before == 0 {
		t.Fatalf("no updates in %v before the restart — fix the setup before testing recovery", manualStreamProof)
	}
	timeline("baseline: %d updates in %v", before, manualStreamProof)

	// Phase 2 — the human does the thing.
	fmt.Printf("\n\n=== %s ===\n", strings.ToUpper(scenario))
	fmt.Printf(">>> %s\n", instruction)
	fmt.Printf(">>> Waiting up to %v for the stream to stop. No keypress needed.\n\n", manualDisruptWait)

	if !waitForStreamStop(ch, manualDisruptWait) {
		t.Fatalf("updates never stopped within %v — the disruption did not happen", manualDisruptWait)
	}
	stoppedAt := time.Now()
	timeline("stream stopped — the session should now be detecting the drop")

	// Phase 3 — the library recovers on its own.
	fmt.Printf(">>> Stream stopped. Bring the PLC back if it is not already.\n")
	fmt.Printf(">>> Waiting up to %v for automatic recovery.\n\n", manualRecoveryGrace)

	recovered := waitForStreamResume(ch, manualRecoveryGrace)
	if !recovered {
		state := sess.lifecycle.state.load()
		t.Fatalf("no updates within %v of the disruption; session state = %v, IsClosed=%v",
			manualRecoveryGrace, state, sess.IsClosed())
	}
	timeline("updates resumed after %v", time.Since(stoppedAt).Round(time.Second))

	// Phase 4 — the session must be genuinely healthy, not merely streaming.
	//
	// Wait for it to settle first. The first update arrives while the recovery is
	// still committing the rest of the batch, so sampling here directly reads a
	// half-finished reconnect: measured against 192.168.3.70 with 40 symbols, this
	// read "state = Reconnecting, bound = 21" one second before the same reconnect
	// logged success with all 40 bound. That is the observer being early, not the
	// session being wrong — but it looks identical to a partial recovery, so the
	// wait has to be explicit rather than a sleep.
	settleDeadline := time.Now().Add(60 * time.Second)
	for {
		state := sess.lifecycle.state.load()
		if state == SessionStateConnected || sess.IsClosed() {
			break
		}
		if time.Now().After(settleDeadline) {
			t.Fatalf("session still %v 60s after updates resumed", state)
		}
		time.Sleep(100 * time.Millisecond)
	}
	timeline("session settled after %v", time.Since(stoppedAt).Round(time.Second))
	if sess.IsClosed() {
		t.Error("session reports closed although updates resumed")
	}
	if state := sess.lifecycle.state.load(); state != SessionStateConnected {
		t.Errorf("state = %v, want Connected after recovery", state)
	}

	sess.notifications.lock.Lock()
	bound := len(sess.notifications.activeNotifications)
	sess.notifications.lock.Unlock()
	if bound != subscribed {
		t.Errorf("bound notifications = %d, want %d — re-subscribe did not restore every symbol", bound, subscribed)
	}

	// Every symbol must stream again, not just the noisy one.
	seen := map[string]bool{}
	deadline := time.After(60 * time.Second)
	for len(seen) < subscribed {
		select {
		case u := <-ch:
			seen[strings.ToLower(u.Variable)] = true
		case <-deadline:
			t.Errorf("after recovery only %d/%d symbols streamed: %v", len(seen), subscribed, seen)
			return
		}
	}
	timeline("all %d symbols streaming again", len(seen))

	// A read on the recovered session must work too — notifications flowing does
	// not prove the request path was restored.
	readCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if _, err := sess.ReadFromSymbol(readCtx, symbol); err != nil {
		t.Errorf("ReadFromSymbol after recovery: %v", err)
	}
	timeline("read path healthy — recovery complete")
}

// drainFor counts updates arriving during d.
func drainFor(ch <-chan *Update, d time.Duration) int {
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

// waitForStreamStop reports whether the update stream went quiet for a stretch
// long enough that the PLC is clearly gone, within limit.
func waitForStreamStop(ch <-chan *Update, limit time.Duration) bool {
	quietFor := 5 * time.Second
	if limit < 30*time.Second {
		// Smoke-test sizing: a five-second quiet threshold would eat the whole
		// budget before it could report anything.
		quietFor = limit / 3
	}
	deadline := time.After(limit)
	for {
		select {
		case <-ch:
			// still streaming; keep waiting
		case <-time.After(quietFor):
			return true
		case <-deadline:
			return false
		}
	}
}

// waitForStreamResume reports whether updates started flowing again within limit.
func waitForStreamResume(ch <-chan *Update, limit time.Duration) bool {
	deadline := time.After(limit)
	for {
		select {
		case <-ch:
			return true
		case <-deadline:
			return false
		}
	}
}
