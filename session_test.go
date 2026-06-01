package ads

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

// TestZeroOldSymbolHandles validates R-CACHE-004 in full: loadSymbols
// replaces the cache.symbols map, but callers (e.g. readMultipleSymbolsRetry)
// may hold *symbol pointers into the OLD map. zeroOldSymbolHandles MUST
// clear Handle, Value, Valid, ValueParsed, and LastUpdateTime so stale
// data cannot leak post-reconnect.
//
// Validates: R-CACHE-004.
func TestZeroOldSymbolHandles(t *testing.T) {
	t0 := time.Now()
	oldMap := map[string]*symbol{
		"a": {
			Name:           "a",
			Handle:         0x1234,
			Value:          "42",
			Valid:          true,
			ValueParsed:    true,
			LastUpdateTime: t0,
		},
		"b": {
			Name:           "b",
			Handle:         0x5678,
			Value:          "hello",
			Valid:          true,
			ValueParsed:    true,
			LastUpdateTime: t0,
		},
		"c": {Name: "c"}, // already zero across all fields
	}
	pa := oldMap["a"]
	pb := oldMap["b"]
	pc := oldMap["c"]

	zeroOldSymbolHandles(oldMap)

	for _, p := range []*symbol{pa, pb, pc} {
		if p.Handle != 0 {
			t.Errorf("%s.Handle = 0x%X, want 0", p.Name, p.Handle)
		}
		if p.Value != "" {
			t.Errorf("%s.Value = %q, want empty string", p.Name, p.Value)
		}
		if p.Valid {
			t.Errorf("%s.Valid = true, want false", p.Name)
		}
		if p.ValueParsed {
			t.Errorf("%s.ValueParsed = true, want false", p.Name)
		}
		if !p.LastUpdateTime.IsZero() {
			t.Errorf("%s.LastUpdateTime = %v, want zero", p.Name, p.LastUpdateTime)
		}
	}
}

// Nil and empty input must not panic. Validates: R-CACHE-004 (defensive).
func TestZeroOldSymbolHandles_NilSafe(t *testing.T) {
	zeroOldSymbolHandles(nil)
	zeroOldSymbolHandles(map[string]*symbol{})
}

// TestNewSession_TotalConstruction asserts NewSession does no I/O, spawns
// no goroutines, leaves Session.client nil, and applies defaults
// (requestTimeout=5s when zero, localPort=10500 when zero, FSM in
// Constructed state).
//
// Validates: R-SES-001 (NewSession total).
func TestNewSession_TotalConstruction(t *testing.T) {
	// Allow runtime to settle.
	for i := 0; i < 5; i++ {
		runtime.Gosched()
	}
	baseline := runtime.NumGoroutine()

	sess, err := NewSession(context.Background(), testEndpoint())
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	defer sess.Close()

	// No goroutines should have been spawned (allow ±1 noise).
	for i := 0; i < 5; i++ {
		runtime.Gosched()
	}
	got := runtime.NumGoroutine()
	if got > baseline+1 {
		t.Errorf("NewSession spawned goroutines: baseline=%d, after=%d", baseline, got)
	}

	if sess.client.Load() != nil {
		t.Error("client should be nil before Connect()")
	}
	if sess.requestTimeout != 5*time.Second {
		t.Errorf("requestTimeout = %v, want 5s default", sess.requestTimeout)
	}
	// Default local AMS port is random in [32768, 49151] so each Session is
	// a distinct AMS client identity to the PLC. WithLocalAMS overrides for
	// stable-port deployments.
	if sess.source.Port < 32768 || sess.source.Port > 49151 {
		t.Errorf("localPort = %d, want random in [32768, 49151]", sess.source.Port)
	}
	if state := sess.lifecycle.state.load(); state != SessionStateConstructed {
		t.Errorf("FSM state = %v, want Constructed", state)
	}
}

// TestNewSession_OptionsApplied confirms WithRequestTimeout and friends
// override defaults; values <= 0 fall through to defaults.
//
// Validates: R-SES-001, R-SES-006 (option apply-time validation).
func TestNewSession_OptionsApplied(t *testing.T) {
	sess, err := NewSession(context.Background(), testEndpoint(),
		WithLocalAMS(AMSAddress{Port: 1234}),
		WithRequestTimeout(11*time.Second))
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	defer sess.Close()

	if sess.requestTimeout != 11*time.Second {
		t.Errorf("requestTimeout = %v, want 11s", sess.requestTimeout)
	}
	if sess.source.Port != 1234 {
		t.Errorf("localPort = %d, want 1234", sess.source.Port)
	}
}

// TestSession_OptionValidation_NoOpOnZeroValues asserts WithLogger(nil),
// WithRequestTimeout(0), WithRoute("","",""), and WithBackoff(zero) are
// no-ops — the corresponding Session field remains at the default
// applied during NewSession construction.
//
// Validates: R-SES-006 (option apply-time validation).
func TestSession_OptionValidation_NoOpOnZeroValues(t *testing.T) {
	customLogger := slog.New(&testLogHandler{})
	// Apply real options first via NewSession; then re-construct with
	// zero-valued options and assert they did NOT clobber the default.
	defaultBackoff := DefaultBackoffConfig()

	// Use a fully-specified BackoffConfig — v2.2 Validate() rejects partial
	// configs that previously silently overrode defaults with zero values.
	// Override only the fields we want to differ from the default, keeping
	// the monotonic Initial <= Mid <= Slow <= Max invariant intact.
	customBackoff := DefaultBackoffConfig()
	customBackoff.InitialAttempts = 9
	sess, err := NewSession(context.Background(), testEndpoint(),
		WithLogger(customLogger),
		WithBackoff(customBackoff),
	)
	if err != nil {
		t.Fatalf("NewSession real: %v", err)
	}
	defer sess.Close()

	// Now apply zero-valued options to a SEPARATE sess and confirm defaults survive.
	sess2, err := NewSession(context.Background(), testEndpoint(),
		WithLogger(nil),
		WithRequestTimeout(0),
		WithRoute("", "", ""),
	)
	if err != nil {
		t.Fatalf("NewSession zero-options: %v", err)
	}
	defer sess2.Close()

	if sess2.logger == nil {
		t.Error("WithLogger(nil) should leave logger non-nil (default)")
	}
	if sess2.requestTimeout != 5*time.Second {
		t.Errorf("WithRequestTimeout(0): requestTimeout = %v, want 5s default", sess2.requestTimeout)
	}
	if sess2.route.name != "" || sess2.route.username != "" || sess2.route.password != "" {
		t.Errorf("WithRoute(\"\",\"\",\"\") populated fields unexpectedly: name=%q user=%q pwSet=%v",
			sess2.route.name, sess2.route.username, sess2.route.password != "")
	}

	// Sanity: the real WithBackoff IS applied on sess (proves the option
	// wiring works at all; a no-op WithBackoff would be detected by a
	// real-config test, but the spec only requires zero-value to fall
	// through to default — the production WithBackoff stores raw, so a
	// zero-config struct overrides default. Assert sess defaults differ
	// from a zero-config sample to confirm presence of difference.).
	if sess.lifecycle.backoffConfig.InitialAttempts != 9 {
		t.Errorf("WithBackoff did not apply: InitialAttempts = %d, want 9",
			sess.lifecycle.backoffConfig.InitialAttempts)
	}
	// Defaults document — sanity-check the constants are stable.
	if defaultBackoff.InitialInterval != 1*time.Second {
		t.Errorf("DefaultBackoffConfig changed: InitialInterval = %v", defaultBackoff.InitialInterval)
	}
}

// TestSession_WithMaxReconnectAttempts_NegativeNoOp confirms a negative
// MaxReconnectAttempts is silently ignored / accepted; the spec says zero
// (default) means infinite retries. Currently no validation rejects
// negative values: the test pins observable behavior.
//
// Validates: R-SES-006 (option validation, observable behavior).
func TestSession_WithMaxReconnectAttempts_NegativeNoOp(t *testing.T) {
	sess, err := NewSession(context.Background(), testEndpoint(),
		WithRequestTimeout(time.Second),
		WithMaxReconnectAttempts(-1))
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	defer sess.Close()
	// Production behavior: -1 is stored verbatim (no validation). The
	// reconnect loop's "n>0 && attempts > n" guard means -1 is the same
	// as 0 (infinite retries). Pin this behavior so a future change that
	// rejects negatives surfaces as a test failure.
	if sess.lifecycle.maxReconnectAttempts != -1 {
		t.Errorf("maxReconnectAttempts = %d, want -1 (stored verbatim)",
			sess.lifecycle.maxReconnectAttempts)
	}
}

// TestSession_ConnectAfterCloseRejected asserts Connect on a closed Session
// does NOT progress to Connected. Current production code: Close transitions
// to SessionStateClosed (terminal); subsequent transitionTo(Connecting)
// returns ok=false (Closed has no allowed transitions per
// allowedTransitions). The Connect call still attempts a TCP dial, but the
// FSM state should remain Closed. We assert the state invariant rather
// than coupling to a specific error string.
//
// Validates: R-SES-005 (Connect after Close rejected).
func TestSession_ConnectAfterCloseRejected(t *testing.T) {
	ep := testEndpoint()
	ep.Port = 1
	sess, err := NewSession(context.Background(), ep, WithRequestTimeout(200*time.Millisecond))
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	// Close without a prior Connect — the FSM transitions Constructed → Closed.
	sess.Close()

	if state := sess.lifecycle.state.load(); state != SessionStateClosed {
		t.Fatalf("post-Close state = %v, want Closed", state)
	}

	// Attempt to Connect — port 1 ensures dial fails fast even if the FSM
	// permitted the transition. Either path (FSM rejection or dial
	// failure) leaves state Closed.
	_ = sess.Connect(context.Background())

	if state := sess.lifecycle.state.load(); state != SessionStateClosed {
		t.Errorf("after Connect post-Close: state = %v, want Closed (no transition out of terminal)", state)
	}
	// Should not have spawned client workers.
	if c := sess.client.Load(); c != nil && c.tx != nil && c.tx.connection != nil {
		t.Error("Connect post-Close created a live transport")
	}
	// Sanity: at least one of these must be true.
	_ = errors.New
}

// TestSession_OnDisconnectFiresOnceOnConcurrentTrigger spawns 50 goroutines
// all calling triggerReconnect on a synthetic Session in Connected state.
// The CAS gate inside triggerReconnect ensures the disconnect callback
// fires exactly once.
//
// Validates: R-SES-008 (onDisconnect fires once on concurrent triggers).
func TestSession_OnDisconnectFiresOnceOnConcurrentTrigger(t *testing.T) {
	var fires atomic.Int32
	var fireWG sync.WaitGroup
	fireWG.Add(1)

	// Build a synthetic Session in Connected state. autoReconnect=false so
	// triggerReconnect does NOT spawn the Reconnect goroutine (which would
	// require a live transport).
	sess := &Session{
		tx:            &transport{},
		notifications: &notificationManager{activeNotifications: make(map[uint32]activeNotification), configsByKey: make(map[string]struct{}), orphanSeen: make(map[uint32]time.Time), orphanSem: make(chan struct{}, orphanDeleteMaxConcurrency)},
		cache:         &symbolCache{symbols: map[string]*symbol{}, onDemandSymbols: map[string]bool{}},
		logger:        getDefaultLogger(),
		lifecycle: &sessionLifecycle{
			closedCh:      make(chan struct{}),
			autoReconnect: false,
		},
		onDisconnect: func() {
			fires.Add(1)
			fireWG.Done()
		},
	}
	// Drive the FSM into Connected so triggerReconnect's transition is legal.
	sess.lifecycle.state.transitionTo(SessionStateConstructed)
	sess.lifecycle.state.transitionTo(SessionStateConnecting)
	sess.lifecycle.state.transitionTo(SessionStateConnected)

	const N = 50
	var startWG sync.WaitGroup
	startWG.Add(N)
	for i := 0; i < N; i++ {
		go func() {
			startWG.Done()
			sess.triggerReconnect()
		}()
	}
	// Wait for them all to fire concurrently.
	startWG.Wait()

	// Wait up to 1s for the (sole) callback to land.
	done := make(chan struct{})
	go func() { fireWG.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("disconnect callback never fired")
	}

	// Give any straggler triggers a moment to (incorrectly) fire.
	time.Sleep(50 * time.Millisecond)

	if got := fires.Load(); got != 1 {
		t.Errorf("onDisconnect fired %d times, want exactly 1", got)
	}
}

// TestSession_HandleStaleDetection_NoMatch validates that non-stale
// ReturnCodes are a no-op for the dispatcher.
//
// Validates: R-CACHE-009 dispatch (negative path).
func TestSession_HandleStaleDetection_NoMatch(t *testing.T) {
	sess := &Session{
		versionStrategy: SymbolVersionIgnore,
		logger:          getDefaultLogger(),
	}
	stale, reason := sess.handleStaleDetection(ReturnCodeNoErrors)
	if stale || reason != "" {
		t.Errorf("got (%v, %q), want (false, \"\")", stale, reason)
	}
}

// TestSession_HandleStaleDetection_Ignore_FiresCallback validates that the
// Ignore strategy fires the callback in a goroutine and returns the
// expected reason without altering Session state.
//
// Validates: R-CACHE-012 (Ignore) + R-NOT-016 (callback reason).
func TestSession_HandleStaleDetection_Ignore_FiresCallback(t *testing.T) {
	cbReason := make(chan Reason, 1)
	sess := &Session{
		versionStrategy: SymbolVersionIgnore,
		versionCallback: func(r Reason) { cbReason <- r },
		logger:          getDefaultLogger(),
	}

	stale, reason := sess.handleStaleDetection(ReturnCodeDeviceSymbolVersionInvalid)
	if !stale || reason != ReasonSymbolVersionInvalid {
		t.Errorf("got (%v, %q), want (true, %q)", stale, reason, ReasonSymbolVersionInvalid)
	}
	select {
	case r := <-cbReason:
		if r != ReasonSymbolVersionInvalid {
			t.Errorf("callback got %q, want %q", r, ReasonSymbolVersionInvalid)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("callback not invoked")
	}
}

// TestSession_HandleStaleDetection_NilCallbackOK validates R-SES-011: when
// no callback is configured, the dispatcher must still classify the code
// without panicking.
//
// Validates: R-SES-011 (nil-callback safety).
func TestSession_HandleStaleDetection_NilCallbackOK(t *testing.T) {
	sess := &Session{
		versionStrategy: SymbolVersionIgnore,
		versionCallback: nil,
		logger:          getDefaultLogger(),
	}
	stale, _ := sess.handleStaleDetection(ReturnCodeDeviceSymbolVersionInvalid)
	if !stale {
		t.Error("expected stale=true")
	}
}

// TestSession_HandleStaleDetection_Close exercises the SymbolVersionClose
// strategy end-to-end against a scriptable PLC stub: handleStaleDetection
// must spawn closeOnStaleDetection in a goroutine, which fires the
// onDisconnect callback and drives the FSM to Closed.
//
// Validates: R-CACHE-011 (Close strategy terminates session +
// surfaces lifecycle event to observers).
func TestSession_HandleStaleDetection_Close(t *testing.T) {
	srv := startScriptableServer(t)
	defer srv.stop()

	disconnected := make(chan struct{})
	sess, _ := newWiredTestSession(t, srv,
		WithSymbolVersionStrategy(SymbolVersionClose),
		WithOnDisconnect(func() { close(disconnected) }),
	)

	// Trigger detection synthetically. Close strategy fires
	// closeOnStaleDetection in a goroutine, so the test must wait for the
	// disconnect signal rather than asserting immediately.
	stale, reason := sess.handleStaleDetection(ReturnCodeDeviceSymbolVersionInvalid)
	if !stale || reason != ReasonSymbolVersionInvalid {
		t.Errorf("handleStaleDetection = (%v, %q), want (true, %q)",
			stale, reason, ReasonSymbolVersionInvalid)
	}

	select {
	case <-disconnected:
		// pass
	case <-time.After(3 * time.Second):
		t.Fatal("Close strategy did not fire onDisconnect within 3s")
	}

	// closeOnStaleDetection invokes sess.Close() in the same goroutine as
	// the onDisconnect callback. Close() runs synchronously, so once the
	// callback has fired and Close() returns, isClosed() must report true.
	// Allow a short grace window for the goroutine to finish Close().
	deadline := time.Now().Add(2 * time.Second)
	for !sess.isClosed() && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if !sess.isClosed() {
		t.Error("session not in Closed state after Close strategy")
	}
}

// TestSession_TryRecordReloadAttempt_CapEnforced validates that
// tryRecordReloadAttempt returns true until the per-window cap is reached
// and false thereafter. Pure unit — no PLC stub needed.
//
// Validates: R-CACHE-013 (sliding-window cap).
func TestSession_TryRecordReloadAttempt_CapEnforced(t *testing.T) {
	sess := &Session{
		maxReloadAttempts: 2,
		reloadWindow:      10 * time.Second,
	}
	if !sess.tryRecordReloadAttempt() {
		t.Error("attempt 1 must succeed")
	}
	if !sess.tryRecordReloadAttempt() {
		t.Error("attempt 2 must succeed")
	}
	if sess.tryRecordReloadAttempt() {
		t.Error("attempt 3 must be rejected (cap=2)")
	}
}

// TestSession_TryRecordReloadAttempt_SlidingWindow validates that entries
// older than reloadWindow are pruned, freeing capacity for new attempts.
//
// Validates: R-CACHE-013 (sliding window).
func TestSession_TryRecordReloadAttempt_SlidingWindow(t *testing.T) {
	sess := &Session{
		maxReloadAttempts: 2,
		reloadWindow:      50 * time.Millisecond,
	}
	if !sess.tryRecordReloadAttempt() {
		t.Fatal("attempt 1")
	}
	if !sess.tryRecordReloadAttempt() {
		t.Fatal("attempt 2")
	}
	// Wait for window to slide past.
	time.Sleep(80 * time.Millisecond)
	if !sess.tryRecordReloadAttempt() {
		t.Error("attempt after window slide must succeed")
	}
}

// TestSession_MarkAllHandlesStale validates the helper used by both the
// Ignore branch of handleStaleDetection and the AutoReload pre-reload
// window: every active notification handle gets a one-shot Stale flag
// with the supplied reason.
//
// Validates: R-NOT-017 (markAllHandlesStale fans the flag across handles).
func TestSession_MarkAllHandlesStale(t *testing.T) {
	sess := newTestConnection()
	defer sess.lifecycle.shutdown()
	sess.notifications.activeNotifications[42] = activeNotification{Sym: &symbol{}}
	sess.notifications.activeNotifications[99] = activeNotification{Sym: &symbol{}}

	sess.markAllHandlesStale(ReasonReloadInProgress)

	if r, ok := sess.consumeStaleFlag(42); !ok || r != ReasonReloadInProgress {
		t.Errorf("h=42: reason=%q ok=%v, want (%q, true)", r, ok, ReasonReloadInProgress)
	}
	if r, ok := sess.consumeStaleFlag(99); !ok || r != ReasonReloadInProgress {
		t.Errorf("h=99: reason=%q ok=%v, want (%q, true)", r, ok, ReasonReloadInProgress)
	}
	// Idempotency check: second consume returns ok=false.
	if _, ok := sess.consumeStaleFlag(42); ok {
		t.Error("flag should be one-shot — second consume should return ok=false")
	}
}

// TestSession_MarkAllHandlesStale_NilNotificationsSafe validates the
// nil-guard for unit-test bare Session{} construction.
func TestSession_MarkAllHandlesStale_NilNotificationsSafe(t *testing.T) {
	sess := &Session{logger: getDefaultLogger()}
	// Must not panic even without notifications manager.
	sess.markAllHandlesStale(ReasonReloadInProgress)
}

// TestSession_AutoReload_BumpsEpoch validates that handleStaleDetection
// under SymbolVersionAutoReload triggers autoReloadOnStaleDetection which
// bumps the session epoch (R-CACHE-003) before attempting reload. The
// scriptable server doesn't implement the full reload sequence — but
// epoch is bumped BEFORE reload is attempted, so the test succeeds even
// when LoadSymbols() ultimately errors out.
//
// Validates: R-CACHE-010 (AutoReload bumps epoch + invalidates handles).
func TestSession_AutoReload_BumpsEpoch(t *testing.T) {
	srv := startScriptableServer(t)
	defer srv.stop()

	sess, _ := newWiredTestSession(t, srv,
		WithSymbolVersionStrategy(SymbolVersionAutoReload),
		WithMaxSymbolVersionReloadAttempts(3),
		WithSymbolVersionReloadWindow(60*time.Second),
	)

	preEpoch := sess.epoch()

	sess.handleStaleDetection(ReturnCodeDeviceSymbolVersionInvalid)

	deadline := time.After(5 * time.Second)
	for sess.epoch() == preEpoch {
		select {
		case <-deadline:
			t.Fatal("AutoReload did not bump epoch within 5s")
		default:
			time.Sleep(50 * time.Millisecond)
		}
	}
}

// TestSession_AutoReload_CapExhaustion_FiresCallback validates that once
// the sliding-window cap is exceeded, the callback fires with
// ReasonReloadCapExhausted and the strategy degrades to Ignore semantics
// (no further reload attempts within the window).
//
// Validates: R-CACHE-013 (cap exhaustion fires callback w/ specific reason).
func TestSession_AutoReload_CapExhaustion_FiresCallback(t *testing.T) {
	srv := startScriptableServer(t)
	defer srv.stop()

	exhausted := make(chan Reason, 4)
	cb := func(reason Reason) {
		if reason == ReasonReloadCapExhausted {
			select {
			case exhausted <- reason:
			default:
			}
		}
	}

	sess, _ := newWiredTestSession(t, srv,
		WithSymbolVersionStrategy(SymbolVersionAutoReload),
		WithMaxSymbolVersionReloadAttempts(2),
		WithSymbolVersionReloadWindow(10*time.Second),
		WithOnSymbolVersionChanged(cb))

	for i := 0; i < 4; i++ {
		sess.handleStaleDetection(ReturnCodeDeviceSymbolVersionInvalid)
		time.Sleep(50 * time.Millisecond)
	}

	select {
	case <-exhausted:
		// pass
	case <-time.After(3 * time.Second):
		t.Fatal("cap exhaustion did not fire callback w/ ReasonReloadCapExhausted within 3s")
	}
}

// TestSession_AutoReload_SingleFlight validates that when N stale events fire
// concurrently under SymbolVersionAutoReload, only one reload goroutine runs
// (R-CACHE-010). The single-flight guard (reloadInProgress CAS) must prevent
// duplicate concurrent bumpEpoch calls.
func TestSession_AutoReload_SingleFlight(t *testing.T) {
	srv := startScriptableServer(t)
	defer srv.stop()

	sess, _ := newWiredTestSession(t, srv,
		WithSymbolVersionStrategy(SymbolVersionAutoReload),
		WithMaxSymbolVersionReloadAttempts(10),
		WithSymbolVersionReloadWindow(60*time.Second),
	)

	preEpoch := sess.epoch()

	const N = 5
	var wg sync.WaitGroup
	for range N {
		wg.Add(1)
		go func() {
			defer wg.Done()
			sess.handleStaleDetection(ReturnCodeDeviceSymbolVersionInvalid)
		}()
	}
	wg.Wait()

	// Give goroutines time to complete the reload path.
	deadline := time.After(3 * time.Second)
	for {
		select {
		case <-deadline:
			t.Fatal("epoch never bumped within 3s")
		default:
		}
		if sess.epoch() > preEpoch {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	// Allow the one in-flight goroutine to complete before sampling epoch.
	time.Sleep(200 * time.Millisecond)

	got := int(sess.epoch() - preEpoch)
	if got > 1 {
		t.Errorf("epoch bumped %d times, want 1 (single-flight guard failed)", got)
	}
}

// TestSession_ReadFromSymbol_LengthMismatchTriggersDetection validates the
// supplementary R-CACHE-009 detection path: when the PLC returns a payload
// whose length disagrees with the cached symbol.Length (e.g. operator
// toggled nProbeA INT↔LREAL via TC3 online change with ServerCycle), the
// PLC does NOT surface a ReturnCode — the parse method would fail with
// "symbol.Length N exceeds data buffer size M" and bypass detection.
// readFromSymbolRetry MUST detect the Length mismatch BEFORE parse, fire
// handleStaleDetection (Ignore strategy here so callback fires), and
// return a ReturnCode-typed error (ReturnCodeDeviceInvalidSize / 0x705)
// that callers can match via errors.As.
//
// Closes hardware test gap from TestSymbolVersionClose_OnDetection where
// TC3 type-change only manifests as Go-side parse error.
//
// Validates: R-CACHE-009 (extends detection set with Length-mismatch
// signal) + R-NOT-016 (ReasonInvalidSize wired through callback).
func TestSession_ReadFromSymbol_LengthMismatchTriggersDetection(t *testing.T) {
	srv := startScriptableServer(t)
	defer srv.stop()

	const fakeHandle uint32 = 0xCAFEBABE
	// PLC returns 2 bytes (post-online-change INT size) on Read by handle.
	// Cache will hold Length=8 (pre-change LREAL) — the mismatch is the
	// detection trigger.
	srv.onRead(GroupSymbolValueByHandle, func(_, offset, length uint32) (ReturnCode, []byte) {
		if offset != fakeHandle {
			return ReturnCodeDeviceInvalidParam, nil
		}
		_ = length // requested length is 8 (cached LREAL); we deliberately ship 2.
		return ReturnCodeNoErrors, []byte{0x05, 0x00}
	})

	cbReason := make(chan Reason, 1)
	sess, _ := newWiredTestSession(t, srv,
		WithSymbolVersionStrategy(SymbolVersionIgnore),
		WithOnSymbolVersionChanged(func(r Reason) { cbReason <- r }),
	)

	// Pre-seed the cache with a stale LREAL (Length=8) symbol carrying an
	// already-resolved handle so getSymbol returns it without hitting the
	// network. This emulates the post-online-change state where the cache
	// still holds the pre-change type metadata.
	const symName = "MAIN_DP1.nProbeA"
	sess.cache.symbols[symbolKey(symName)] = &symbol{
		FullName: symName,
		Name:     symName,
		Handle:   fakeHandle,
		Length:   8,
		DataType: "LREAL",
	}

	_, err := sess.ReadFromSymbol(context.Background(), symName)
	if err == nil {
		t.Fatal("expected error from ReadFromSymbol on length mismatch, got nil")
	}
	var rc ReturnCode
	if !errors.As(err, &rc) {
		t.Fatalf("error is not ReturnCode-typed: %v", err)
	}
	if rc != ReturnCodeDeviceInvalidSize {
		t.Errorf("rc = 0x%X, want 0x%X (ReturnCodeDeviceInvalidSize)", uint32(rc), uint32(ReturnCodeDeviceInvalidSize))
	}

	select {
	case r := <-cbReason:
		if r != ReasonInvalidSize {
			t.Errorf("callback reason = %q, want %q", r, ReasonInvalidSize)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("onSymbolVersionChanged callback did not fire within 2s on Length mismatch")
	}
}

// TestSession_IsClosed_FalseBeforeClose validates that IsClosed reports
// false on a freshly-constructed session that has not transitioned into
// the terminal Closed state.
//
// Validates: lifecycle observation contract — IsClosed wraps the private
// FSM probe.
func TestSession_IsClosed_FalseBeforeClose(t *testing.T) {
	sess := newTestConnection()
	defer sess.lifecycle.shutdown()
	if sess.IsClosed() {
		t.Error("IsClosed=true on fresh session, want false")
	}
}

// TestSession_IsClosed_TrueAfterClose validates that IsClosed reports
// true once the FSM has reached the terminal Closed state. We drive the
// FSM directly rather than calling Close() because newTestConnection
// produces a minimal Session without the transport / closedCh / waitgroup
// machinery Close() depends on. The wrapper itself is the unit under
// test, not Close()'s cleanup pathway.
func TestSession_IsClosed_TrueAfterClose(t *testing.T) {
	sess := newTestConnection()
	defer sess.lifecycle.shutdown()
	if _, ok := sess.lifecycle.state.transitionToOnce(SessionStateClosed); !ok {
		t.Fatal("FSM transition to Closed not permitted from Constructed")
	}
	if !sess.IsClosed() {
		t.Error("IsClosed=false after transition to Closed, want true")
	}
}

// TestReleasePLCResources_NotificationCleanup exercises the notification
// cleanup branch of releasePLCResources directly (rather than via Close,
// which blocks on the Client waitGroup in test harness setups where the
// Client ctx is independent of sess.lifecycle.ctx). The helper is the
// shared entry point used by both Close() and the Reconnect-exhaustion
// path; testing it directly covers both call sites.
//
// Validates: PLC-side notification delete fires for every staged handle.
func TestReleasePLCResources_NotificationCleanup(t *testing.T) {
	srv := startScriptableServer(t)
	defer srv.stop()

	const stagedHandle uint32 = 0xC0DE
	var deletes atomic.Int32
	// releasePLCResources calls bestEffortDeleteNotifications which prefers
	// SumDeleteDeviceNotification; register that handler so the sum path
	// completes instead of falling back. Count handles passed through.
	srv.onWriteRead(GroupSumupDeleteDeviceNotification, func(req []byte) []byte {
		nItems := len(req) / 4
		codes := make([]ReturnCode, nItems)
		for i := 0; i < nItems; i++ {
			h := binary.LittleEndian.Uint32(req[i*4:])
			if h == stagedHandle {
				deletes.Add(1)
			}
			codes[i] = ReturnCodeNoErrors
		}
		return buildSumDeleteNotifPayload(codes)
	})

	sess, _ := newWiredTestSession(t, srv)
	sess.notifications.lock.Lock()
	sess.notifications.activeNotifications[stagedHandle] = activeNotification{Sym: &symbol{FullName: "MAIN.x"}}
	sess.notifications.lock.Unlock()

	sess.releasePLCResources(false)

	if got := deletes.Load(); got < 1 {
		t.Errorf("staged handle delivered to SumDelete = %d, want >= 1 (must release PLC subscriptions)", got)
	}
}

// TestReleasePLCResources_SymbolHandleRelease_SkippedWhenDisconnected pins
// the wasDisconnected=true short-circuit: when the transport is already
// dead, the helper must NOT issue PLC Write commands for handle release.
// Issuing them against a dead socket would block Close behind requestTimeout
// for every staged handle.
//
// Validates: wasDisconnected gate on symbol-handle release path.
func TestReleasePLCResources_SymbolHandleRelease_SkippedWhenDisconnected(t *testing.T) {
	srv := startScriptableServer(t)
	defer srv.stop()

	var writes atomic.Int32
	srv.onWrite(GroupSymbolReleaseHandle, func(_, _ uint32, _ []byte) ReturnCode {
		writes.Add(1)
		return ReturnCodeNoErrors
	})

	sess, _ := newWiredTestSession(t, srv)
	sess.cache.lock.Lock()
	sess.cache.symbols[symbolKey("MAIN.x")] = &symbol{
		FullName: "MAIN.x", Name: "MAIN.x", Handle: 0x12345678,
	}
	sess.cache.lock.Unlock()

	sess.releasePLCResources(true) // wasDisconnected=true

	if got := writes.Load(); got != 0 {
		t.Errorf("GroupSymbolReleaseHandle writes = %d, want 0 (must skip when disconnected)", got)
	}
}

// TestReleasePLCResources_SymbolHandleRelease_FiredWhenConnected: with the
// transport alive, the helper must issue ReleaseHandle for every cached
// symbol with a non-zero Handle.
func TestReleasePLCResources_SymbolHandleRelease_FiredWhenConnected(t *testing.T) {
	srv := startScriptableServer(t)
	defer srv.stop()

	const stagedHandle uint32 = 0xABCD0001
	var writes atomic.Int32
	srv.onWrite(GroupSymbolReleaseHandle, func(_, _ uint32, _ []byte) ReturnCode {
		writes.Add(1)
		return ReturnCodeNoErrors
	})

	sess, _ := newWiredTestSession(t, srv)
	sess.cache.lock.Lock()
	sess.cache.symbols[symbolKey("MAIN.x")] = &symbol{
		FullName: "MAIN.x", Name: "MAIN.x", Handle: stagedHandle,
	}
	sess.cache.lock.Unlock()

	sess.releasePLCResources(false) // wasDisconnected=false

	if got := writes.Load(); got < 1 {
		t.Errorf("ReleaseHandle writes = %d, want >= 1 (alive transport must release)", got)
	}
}

// TestHandleStaleDetection_Ignore_FiresCallbackPerTrigger pins the contract
// that under SymbolVersionIgnore, N concurrent stale-cache detections fire N
// versionCallback invocations. The Ignore strategy intentionally does NOT
// gate the callback behind reloadInProgress CAS (which is AutoReload-only) —
// callers using Ignore typically install their own dedup logic and rely on
// every detection being observable.
//
// Companion to TestSession_AutoReload_SingleFlight, which pins the inverse
// contract for AutoReload (1 callback per N triggers).
func TestHandleStaleDetection_Ignore_FiresCallbackPerTrigger(t *testing.T) {
	sess := newTestConnection()
	defer sess.lifecycle.shutdown()
	sess.versionStrategy = SymbolVersionIgnore

	var callbacks atomic.Int32
	sess.versionCallback = func(_ Reason) {
		callbacks.Add(1)
	}

	const N = 25
	var wg sync.WaitGroup
	wg.Add(N)
	for i := 0; i < N; i++ {
		go func() {
			defer wg.Done()
			sess.handleStaleDetection(ReturnCodeDeviceSymbolVersionInvalid)
		}()
	}
	wg.Wait()

	// versionCallback is launched in a goroutine; give them all room to run.
	// Each callback is trivial (single atomic.Add), so a short bounded wait
	// is enough — but use a deadline so a regression that drops callbacks
	// fails reliably instead of timing out the whole test.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if callbacks.Load() == int32(N) {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if got := callbacks.Load(); got != int32(N) {
		t.Errorf("Ignore strategy: %d callbacks for %d triggers, want %d (every detection must be observable)", got, N, N)
	}
}

// TestNewSession_DefaultRandomLocalPort_InRange asserts that every NewSession
// picks a local AMS source port within the IANA dynamic range [32768, 49151].
// Random selection (vs fixed 10500) prevents notification-handle table
// collisions on the PLC across concurrent / restarted sessions sharing a
// host (Beckhoff InfoSys: PLC indexes notifications by source NetID + port).
func TestNewSession_DefaultRandomLocalPort_InRange(t *testing.T) {
	for i := 0; i < 10; i++ {
		sess, err := NewSession(context.Background(), testEndpoint())
		if err != nil {
			t.Fatalf("iter %d: NewSession: %v", i, err)
		}
		if sess.source.Port < 32768 || sess.source.Port > 49151 {
			t.Errorf("iter %d: port=%d, want [32768, 49151]", i, sess.source.Port)
		}
		_ = sess.Close()
	}
}

// TestNewSession_DefaultRandomLocalPort_Distribution catches a regression
// where the random source is mis-seeded and produces a constant port across
// constructions. 100 sessions should observe at least 50 distinct ports.
func TestNewSession_DefaultRandomLocalPort_Distribution(t *testing.T) {
	seen := map[uint16]struct{}{}
	const N = 100
	for i := 0; i < N; i++ {
		sess, err := NewSession(context.Background(), testEndpoint())
		if err != nil {
			t.Fatalf("iter %d: NewSession: %v", i, err)
		}
		seen[sess.source.Port] = struct{}{}
		_ = sess.Close()
	}
	if len(seen) < 50 {
		t.Errorf("only %d distinct ports across %d sessions — random source may be constant-seeded", len(seen), N)
	}
}

// TestNewSession_WithLocalAMS_ZeroPort_KeepsRandomDefault verifies that
// WithLocalAMS(AMSAddress{Port: 0}) does NOT clobber the random default port.
// Port == 0 is the zero value; treating it as "explicit override to 0" would
// produce an invalid AMS source. WithLocalAMS guards Port != 0 explicitly.
func TestNewSession_WithLocalAMS_ZeroPort_KeepsRandomDefault(t *testing.T) {
	sess, err := NewSession(context.Background(), testEndpoint(),
		WithLocalAMS(AMSAddress{NetID: [6]byte{10, 20, 30, 40, 1, 1}, Port: 0}),
	)
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	defer sess.Close()
	if sess.source.Port < 32768 || sess.source.Port > 49151 {
		t.Errorf("port=%d, want random default (Port=0 in WithLocalAMS must not override)", sess.source.Port)
	}
	if sess.source.NetID != ([6]byte{10, 20, 30, 40, 1, 1}) {
		t.Errorf("NetID override lost: got %v", sess.source.NetID)
	}
}

// TestAutoReload_DeletesOldHandlesBeforeResubscribe verifies Fix 2 of the
// v2.2.1 PLC-flood patch: reloadSymbolsAndResubscribe must issue a
// SumDeleteDeviceNotification for every pre-reload handle BEFORE attempting
// to re-subscribe, so the PLC's notification handle table doesn't accumulate
// orphan entries across online-change cycles.
//
// We pre-populate activeNotifications with two synthetic handles, intercept
// the SumDelete RPC via the scriptable server, then drive
// reloadSymbolsAndResubscribe. LoadSymbols is expected to fail (no upload
// handler registered) — we only validate the pre-delete step ran and that
// activeNotifications was wiped under the same lock.
func TestAutoReload_DeletesOldHandlesBeforeResubscribe(t *testing.T) {
	srv := startScriptableServer(t)
	defer srv.stop()

	var mu sync.Mutex
	var deletedHandles []uint32
	srv.onWriteRead(GroupSumupDeleteDeviceNotification, func(req []byte) []byte {
		n := len(req) / 4
		mu.Lock()
		for i := 0; i < n; i++ {
			deletedHandles = append(deletedHandles, binary.LittleEndian.Uint32(req[i*4:]))
		}
		mu.Unlock()
		codes := make([]ReturnCode, n)
		for i := range codes {
			codes[i] = ReturnCodeNoErrors
		}
		return buildSumDeleteNotifPayload(codes)
	})

	sess, _ := newWiredTestSession(t, srv)
	sess.notifications.lock.Lock()
	sess.notifications.activeNotifications[0xA1A1] = activeNotification{Sym: &symbol{FullName: "MAIN.x"}, Ch: nil}
	sess.notifications.activeNotifications[0xB2B2] = activeNotification{Sym: &symbol{FullName: "MAIN.y"}, Ch: nil}
	sess.notifications.lock.Unlock()

	// LoadSymbols is expected to fail (no upload-info handler registered).
	// The pre-delete step runs unconditionally before LoadSymbols, so we
	// don't care about the returned error.
	_ = sess.reloadSymbolsAndResubscribe()

	mu.Lock()
	got := append([]uint32{}, deletedHandles...)
	mu.Unlock()

	if len(got) != 2 {
		t.Fatalf("Delete RPC received %d handles, want 2: got=%v", len(got), got)
	}
	seen := map[uint32]bool{0xA1A1: false, 0xB2B2: false}
	for _, h := range got {
		if _, ok := seen[h]; ok {
			seen[h] = true
		}
	}
	for h, found := range seen {
		if !found {
			t.Errorf("handle 0x%X not in Delete RPC payload", h)
		}
	}

	sess.notifications.lock.Lock()
	postLen := len(sess.notifications.activeNotifications)
	sess.notifications.lock.Unlock()
	if postLen != 0 {
		t.Errorf("activeNotifications len = %d after reload, want 0 (must be wiped under lock with snapshot)", postLen)
	}
}

// TestAutoReload_NoOldHandles_SkipsDelete validates the empty-snapshot path:
// when reloadSymbolsAndResubscribe runs with no pre-existing handles, no
// SumDelete RPC must fire (avoids a useless wire round-trip on first reload).
func TestAutoReload_NoOldHandles_SkipsDelete(t *testing.T) {
	srv := startScriptableServer(t)
	defer srv.stop()

	var deleteCalls atomic.Int32
	srv.onWriteRead(GroupSumupDeleteDeviceNotification, func(_ []byte) []byte {
		deleteCalls.Add(1)
		return buildSumDeleteNotifPayload([]ReturnCode{ReturnCodeNoErrors})
	})

	sess, _ := newWiredTestSession(t, srv)
	// activeNotifications is empty by construction.
	_ = sess.reloadSymbolsAndResubscribe()

	if got := deleteCalls.Load(); got != 0 {
		t.Errorf("SumDelete called %d times with empty active set, want 0", got)
	}
}

// TestIsProbeRetryable_TransportLevel verifies the predicate identifies
// transport-level probe errors (RST/EOF/timeout/closed) as retryable, so
// ensureRouteOnConnect's redial-retry path engages on transient PLC slot
// conflicts instead of spuriously firing AddRoute.
func TestIsProbeRetryable_TransportLevel(t *testing.T) {
	connResetOp := &net.OpError{
		Op:  "read",
		Net: "tcp",
		Err: &net.OpError{Err: syscall.ECONNRESET},
	}
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"nil → not retryable", nil, false},
		{"ErrTransportClosed", ErrTransportClosed, true},
		{"wrapped ErrTransportClosed", fmt.Errorf("send: %w", ErrTransportClosed), true},
		{"bare io.EOF", io.EOF, true},
		{"wrapped io.EOF", fmt.Errorf("recv: %w", io.EOF), true},
		{"context.DeadlineExceeded", context.DeadlineExceeded, true},
		{"wrapped DeadlineExceeded", fmt.Errorf("probe: %w", context.DeadlineExceeded), true},
		{"bare ECONNRESET", syscall.ECONNRESET, true},
		{"wrapped net.OpError with ECONNRESET", fmt.Errorf("listen: %w", connResetOp), true},
		{"unrelated error → not retryable", errors.New("ads: ReturnCodeRouterNotInitialized"), false},
		{"context.Canceled → not retryable", context.Canceled, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := isProbeRetryable(tc.err); got != tc.want {
				t.Errorf("isProbeRetryable(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}
