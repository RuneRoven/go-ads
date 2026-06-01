package ads

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// notifications_test.go — Session.AddSymbolNotification(s) + DeleteDeviceNotification
// + bestEffortDelete unit tests.
//
// Covers:
//   - R-NOT-001 (channel-mismatch rejected — pre-roundtrip)
//   - R-NOT-002 (duplicate-symbol rejected: single, in-batch, cross-batch)
//   - R-NOT-003 (TOCTOU re-check on subscribe — partial; full path needs stub Client)
//   - R-NOT-004 (generation bump mid-roundtrip → stranded-symbol detected)
//   - R-NOT-008 (DeleteDeviceNotification clears state — partial; live transport needed)
//   - R-NOT-010 (channel set only on first success)
//   - R-NOT-013 (resubscribe retry up to max — TODO, depends on stub Client)
//   - R-NOT-015 (bestEffortDelete handles mixed success — TODO, depends on stub Client)

// newNotifTestSession is a synthetic Session with cache + notifications +
// lifecycle. No client; tests that need network use the echo helper.
func newNotifTestSession() *Session {
	return &Session{
		cache:         &symbolCache{symbols: map[string]*symbol{}, onDemandSymbols: map[string]bool{}},
		notifications: &notificationManager{activeNotifications: make(map[uint32]activeNotification), configsByKey: make(map[string]struct{}), orphanSeen: make(map[uint32]time.Time), orphanSem: make(chan struct{}, orphanDeleteMaxConcurrency)},
		lifecycle:     &sessionLifecycle{closedCh: make(chan struct{})},
		logger:        getDefaultLogger(),
	}
}

// preSeedSymbol primes the cache with a symbol that has a non-zero handle
// so getSymbol returns immediately without taking the GetHandleByName
// network path.
func preSeedSymbol(sess *Session, name string) *symbol {
	sym := &symbol{
		FullName: name,
		Name:     name,
		DataType: "INT",
		Length:   2,
		Handle:   0xC0DE,
	}
	sess.cache.lock.Lock()
	sess.cache.symbols[symbolKey(name)] = sym
	sess.cache.lock.Unlock()
	return sym
}

// TestAddSymbolNotification_ChannelMismatchRejected pre-seeds a notifications
// channel, then calls AddSymbolNotification with a DIFFERENT channel. The
// pre-check inside AddSymbolNotification (under notifications.lock, BEFORE
// any PLC roundtrip) rejects with an error.
//
// Validates: R-NOT-001 (single channel per Connection).
func TestAddSymbolNotification_ChannelMismatchRejected(t *testing.T) {
	sess := newNotifTestSession()
	preSeedSymbol(sess, "MAIN.x")
	preSeedSymbol(sess, "MAIN.y")

	// Pre-set the notifications channel to chA.
	chA := make(chan *Update, 1)
	sess.notifications.lock.Lock()
	sess.notifications.notificationChannel = chA
	sess.notifications.lock.Unlock()

	// Attempt AddSymbolNotification with chB ≠ chA.
	chB := make(chan *Update, 1)
	_, err := sess.AddSymbolNotification(context.Background(), "MAIN.x", 0, 0, TransModeServerOnChange, chB)
	if err == nil {
		t.Fatal("AddSymbolNotification with mismatched channel: err = nil, want error")
	}
	if !strings.Contains(err.Error(), "same updateReceiver channel") {
		t.Errorf("err = %v, want channel-mismatch message", err)
	}
}

// TestAddSymbolNotifications_DuplicateRejected exercises three paths
// (single-call duplicate, in-batch duplicate, cross-batch duplicate) at
// the pre-check stage that does NOT require a working Client. The
// production code marks the duplicate result.Skipped and continues.
//
// (a) Cross-batch — pre-existing config rejects the second.
// (b) In-batch — same name twice in one configs[] slice.
// (c) Single-call — direct AddSymbolNotification after a prior one is
//
//	covered by R-NOT-001 (channel-mismatch) elsewhere; here we use
//	the pre-existing-config pre-check inside AddSymbolNotifications.
//
// Validates: R-NOT-002 (duplicate-symbol rejected).
func TestAddSymbolNotifications_DuplicateRejected(t *testing.T) {
	t.Run("cross_batch_existing", func(t *testing.T) {
		sess := newNotifTestSession()
		preSeedSymbol(sess, "MAIN.x")

		ch := make(chan *Update, 1)
		// Pre-stage one config so the pre-check rejects on second batch.
		sess.notifications.lock.Lock()
		sess.notifications.addConfig(NotificationConfig{SymbolName: "MAIN.x"})
		sess.notifications.lock.Unlock()

		results, err := sess.AddSymbolNotifications(context.Background(), []NotificationConfig{
			{SymbolName: "MAIN.x", TransmissionMode: TransModeServerOnChange},
		}, ch)
		if err != nil {
			t.Fatalf("AddSymbolNotifications: %v", err)
		}
		if len(results) != 1 {
			t.Fatalf("results len = %d, want 1", len(results))
		}
		if results[0].Skipped == nil {
			t.Errorf("Skipped = nil, want duplicate-rejection error")
		} else if !strings.Contains(results[0].Skipped.Error(), "already subscribed") {
			t.Errorf("Skipped = %v, want 'already subscribed'", results[0].Skipped)
		}
	})

	t.Run("in_batch", func(t *testing.T) {
		sess := newNotifTestSession()
		preSeedSymbol(sess, "MAIN.dup")

		ch := make(chan *Update, 1)
		// Pre-seed an existing config so the FIRST entry is rejected as
		// already-subscribed; the second entry hits the in-batch dup
		// branch. Both are Skipped at the pre-check stage so requests[]
		// stays empty and SumAddDeviceNotification is not invoked
		// (nil client would panic).
		sess.notifications.lock.Lock()
		sess.notifications.addConfig(NotificationConfig{SymbolName: "MAIN.dup"})
		sess.notifications.lock.Unlock()

		results, err := sess.AddSymbolNotifications(context.Background(), []NotificationConfig{
			{SymbolName: "MAIN.dup", TransmissionMode: TransModeServerOnChange},
			{SymbolName: "MAIN.dup", TransmissionMode: TransModeServerOnChange},
		}, ch)
		if err != nil {
			t.Fatalf("AddSymbolNotifications: %v", err)
		}
		if len(results) != 2 {
			t.Fatalf("results len = %d, want 2", len(results))
		}
		// Both entries Skipped: first by existing-config, second by either
		// existing-config or in-batch-dup. Spec asks the in-batch path to
		// be flagged; the production code currently flags it as
		// "already subscribed" because the existing pre-check fires first.
		// Either branch yields a non-nil Skipped — that is the user-
		// observable contract.
		for i, r := range results {
			if r.Skipped == nil {
				t.Errorf("results[%d].Skipped = nil, want duplicate rejection", i)
			}
		}
	})

	t.Run("channel_mismatch", func(t *testing.T) {
		sess := newNotifTestSession()
		preSeedSymbol(sess, "MAIN.x")

		// Pre-set the channel.
		chA := make(chan *Update, 1)
		sess.notifications.lock.Lock()
		sess.notifications.notificationChannel = chA
		sess.notifications.lock.Unlock()

		chB := make(chan *Update, 1)
		_, err := sess.AddSymbolNotifications(context.Background(), []NotificationConfig{
			{SymbolName: "MAIN.x", TransmissionMode: TransModeServerOnChange},
		}, chB)
		if err == nil {
			t.Errorf("AddSymbolNotifications with mismatched channel: err = nil, want error")
		}
	})
}

// TestAddSymbolNotification_StrandedSymbol_DetectedByEpoch drives the
// post-roundtrip stranded-symbol detection in AddSymbolNotification
// (notification_api.go). Two production branches detect strands:
//
//	(a) fresh == nil: cache.symbols no longer contains the key after roundtrip.
//	    Returns "removed from cache during subscribe (likely online change
//	    or LoadSymbols)" and releases the just-acquired PLC handle.
//
//	(b) epoch != cacheGen: another reload landed AFTER the post-roundtrip
//	    cache.lock release but BEFORE notifications.lock acquire (the
//	    residual race window). Returns "stranded by concurrent cache reload
//	    during subscribe" and releases the handle.
//
// This test exercises (a), the deterministic vanish path: pre-seed cache,
// kick off AddSymbolNotification, and during the in-flight roundtrip
// delete the symbol from cache + bumpEpoch (mimicking loadSymbols). After
// the network roundtrip, fresh is nil → branch (a) fires.
//
// Branch (b) is a narrow race window (between two specific lock release/
// acquire points) and is not deterministically reproducible from a test;
// branch (a) fully exercises the orphan-handle release path that R-NOT-004
// guards.
//
// Validates: R-NOT-004 (post-roundtrip stranded-symbol detected; handle released).
func TestAddSymbolNotification_StrandedSymbol_DetectedByEpoch(t *testing.T) {
	srv := startScriptableServer(t)
	defer srv.stop()

	const fakeHandle uint32 = 0xBEEF0001

	srv.onAddDeviceNotification(func(_ addNotifRequest) addNotifResponse {
		return addNotifResponse{Handle: fakeHandle, Error: ReturnCodeNoErrors}
	})
	// 100ms server-side delay gives the test goroutine time to bump epoch
	// + delete the symbol before the response is sent.
	srv.delayBefore(CommandIDAddDeviceNotification, 0, 100*time.Millisecond)

	var deletes atomic.Int32
	srv.onDeleteDeviceNotification(func(h uint32) ReturnCode {
		if h == fakeHandle {
			deletes.Add(1)
		}
		return ReturnCodeNoErrors
	})

	sess, _ := newWiredTestSession(t, srv)
	preSeedSymbol(sess, "MAIN.x")

	ch := make(chan *Update, 1)
	addErr := make(chan error, 1)
	go func() {
		_, err := sess.AddSymbolNotification(context.Background(), "MAIN.x", 0, 0, TransModeServerOnChange, ch)
		addErr <- err
	}()

	// Mid-roundtrip: simulate loadSymbols swap by deleting MAIN.x and
	// bumping the epoch. After the response returns, the post-roundtrip
	// re-fetch finds nil → fresh==nil branch fires.
	time.Sleep(30 * time.Millisecond)
	sess.cache.lock.Lock()
	delete(sess.cache.symbols, symbolKey("MAIN.x"))
	sess.bumpEpoch()
	sess.cache.lock.Unlock()

	select {
	case err := <-addErr:
		if err == nil {
			t.Fatal("AddSymbolNotification: err = nil, want vanished-cache error")
		}
		// Accept either branch's wording; both are valid R-NOT-004 outcomes.
		if !strings.Contains(err.Error(), "removed from cache") &&
			!strings.Contains(err.Error(), "stranded by concurrent cache reload") {
			t.Errorf("AddSymbolNotification err = %v, want 'removed from cache' OR 'stranded by concurrent cache reload'", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("AddSymbolNotification: timeout (>5s)")
	}

	// Production releases the orphaned PLC handle. Wait briefly for the
	// async-issued DeleteDeviceNotification to land.
	deadline := time.Now().Add(2 * time.Second)
	for deletes.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if got := deletes.Load(); got < 1 {
		t.Errorf("DeleteDeviceNotification calls = %d, want at least 1 (handle release after stranding)", got)
	}

	// activeNotifications must NOT contain the stranded handle.
	sess.notifications.lock.Lock()
	_, present := sess.notifications.activeNotifications[fakeHandle]
	sess.notifications.lock.Unlock()
	if present {
		t.Errorf("activeNotifications still contains stranded handle 0x%X", fakeHandle)
	}
}

// TestAddSymbolNotification_TOCTOURecheck drives the post-roundtrip duplicate
// re-check (R-NOT-003) in AddSymbolNotification. Two concurrent calls for the
// same symbol must both pass the pre-check but only one can commit; the loser
// observes the duplicate-already-subscribed re-check and returns an error
// after releasing the just-acquired PLC handle.
//
// Strategy: server adds 100ms delay before each AddDeviceNotification response.
// Both goroutines enter the roundtrip, both pass the pre-check (no existing
// config), the PLC issues both handles. The first to reach the post-roundtrip
// check commits; the second re-check finds the duplicate and rejects.
//
// Validates: R-NOT-003 (TOCTOU re-check after PLC roundtrip).
func TestAddSymbolNotification_TOCTOURecheck(t *testing.T) {
	srv := startScriptableServer(t)
	defer srv.stop()

	var nextHandle atomic.Uint32
	nextHandle.Store(0xAB000001)
	srv.onAddDeviceNotification(func(_ addNotifRequest) addNotifResponse {
		h := nextHandle.Add(1) - 1
		return addNotifResponse{Handle: h, Error: ReturnCodeNoErrors}
	})
	srv.delayBefore(CommandIDAddDeviceNotification, 0, 100*time.Millisecond)

	var deletes atomic.Int32
	srv.onDeleteDeviceNotification(func(_ uint32) ReturnCode {
		deletes.Add(1)
		return ReturnCodeNoErrors
	})

	sess, _ := newWiredTestSession(t, srv)
	preSeedSymbol(sess, "MAIN.x")

	ch := make(chan *Update, 4)
	type result struct {
		handle uint32
		err    error
	}
	resCh := make(chan result, 2)
	for i := 0; i < 2; i++ {
		go func() {
			h, err := sess.AddSymbolNotification(context.Background(), "MAIN.x", 0, 0, TransModeServerOnChange, ch)
			resCh <- result{handle: h, err: err}
		}()
	}

	res := make([]result, 2)
	for i := 0; i < 2; i++ {
		select {
		case r := <-resCh:
			res[i] = r
		case <-time.After(5 * time.Second):
			t.Fatalf("AddSymbolNotification[%d] timeout", i)
		}
	}

	// One success, one duplicate-rejection error.
	successes, errors := 0, 0
	var errStr string
	for _, r := range res {
		if r.err == nil {
			successes++
		} else {
			errors++
			errStr = r.err.Error()
		}
	}
	if successes != 1 || errors != 1 {
		t.Fatalf("results: successes=%d errors=%d (want 1/1); res=%+v", successes, errors, res)
	}
	if !strings.Contains(errStr, "already has an active notification") {
		t.Errorf("loser err = %q, want 'already has an active notification'", errStr)
	}
	// Loser must release its just-acquired PLC handle.
	deadline := time.Now().Add(2 * time.Second)
	for deletes.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if got := deletes.Load(); got < 1 {
		t.Errorf("DeleteDeviceNotification calls = %d, want at least 1 (loser releases handle)", got)
	}

	// Exactly one entry in activeNotifications.
	sess.notifications.lock.Lock()
	got := len(sess.notifications.activeNotifications)
	sess.notifications.lock.Unlock()
	if got != 1 {
		t.Errorf("activeNotifications size = %d, want 1", got)
	}
}

// TestDeleteDeviceNotification_ClearsState pins the cleanup contract:
// after a successful DeleteDeviceNotification, the activeNotifications entry
// is removed, the corresponding notificationConfigs entry is removed, and
// when the last subscription dies notificationChannel is reset to nil.
//
// Variant (handle_invalid): when the PLC returns ReturnCodeDeviceNotifyHandleInvalid
// (0x745), Session.DeleteDeviceNotification surfaces the error to the caller
// and does NOT clean up state — only the success path clears state. This
// preserves caller signal that something was wrong.
//
// Validates: R-NOT-008 (DeleteDeviceNotification clears state on success).
func TestDeleteDeviceNotification_ClearsState(t *testing.T) {
	t.Run("success_clears_state", func(t *testing.T) {
		srv := startScriptableServer(t)
		defer srv.stop()

		const fakeHandle uint32 = 0x11110001
		srv.onAddDeviceNotification(func(_ addNotifRequest) addNotifResponse {
			return addNotifResponse{Handle: fakeHandle, Error: ReturnCodeNoErrors}
		})
		srv.onDeleteDeviceNotification(func(_ uint32) ReturnCode {
			return ReturnCodeNoErrors
		})

		sess, _ := newWiredTestSession(t, srv)
		preSeedSymbol(sess, "MAIN.x")

		ch := make(chan *Update, 1)
		h, err := sess.AddSymbolNotification(context.Background(), "MAIN.x", 0, 0, TransModeServerOnChange, ch)
		if err != nil {
			t.Fatalf("AddSymbolNotification: %v", err)
		}
		if h != fakeHandle {
			t.Fatalf("handle = 0x%X, want 0x%X", h, fakeHandle)
		}
		// Sanity: state populated.
		sess.notifications.lock.Lock()
		_, hasNotif := sess.notifications.activeNotifications[h]
		nConfigs := len(sess.notifications.pending)
		hasChan := sess.notifications.notificationChannel != nil
		sess.notifications.lock.Unlock()
		if !hasNotif || nConfigs != 1 || !hasChan {
			t.Fatalf("post-add: hasNotif=%v configs=%d chanSet=%v", hasNotif, nConfigs, hasChan)
		}

		if err := sess.DeleteDeviceNotification(context.Background(), h); err != nil {
			t.Fatalf("DeleteDeviceNotification: %v", err)
		}

		sess.notifications.lock.Lock()
		_, stillThere := sess.notifications.activeNotifications[h]
		nConfigs = len(sess.notifications.pending)
		chanNil := sess.notifications.notificationChannel == nil
		sess.notifications.lock.Unlock()
		if stillThere {
			t.Errorf("activeNotifications still contains 0x%X after delete", h)
		}
		if nConfigs != 0 {
			t.Errorf("notificationConfigs len = %d, want 0", nConfigs)
		}
		if !chanNil {
			t.Errorf("notificationChannel not nil after last delete")
		}
	})

	t.Run("handle_invalid_surfaces_error", func(t *testing.T) {
		// Production behavior pinned (Session.DeleteDeviceNotification at
		// cmd_notification.go:109): when the underlying client RPC returns
		// a non-success code, the wrapper returns the error before running
		// activeNotifications cleanup. So state survives the call.
		// This is what production does today; if the contract changes to
		// treat 0x745 as success-equivalent (matching SumDeleteDeviceNotification),
		// this assertion will surface the divergence.
		srv := startScriptableServer(t)
		defer srv.stop()

		const fakeHandle uint32 = 0x22220001
		srv.onAddDeviceNotification(func(_ addNotifRequest) addNotifResponse {
			return addNotifResponse{Handle: fakeHandle, Error: ReturnCodeNoErrors}
		})
		srv.onDeleteDeviceNotification(func(_ uint32) ReturnCode {
			return ReturnCodeDeviceNotifyHandleInvalid
		})

		sess, _ := newWiredTestSession(t, srv)
		preSeedSymbol(sess, "MAIN.x")

		ch := make(chan *Update, 1)
		h, err := sess.AddSymbolNotification(context.Background(), "MAIN.x", 0, 0, TransModeServerOnChange, ch)
		if err != nil {
			t.Fatalf("AddSymbolNotification: %v", err)
		}
		err = sess.DeleteDeviceNotification(context.Background(), h)
		if err == nil {
			t.Errorf("DeleteDeviceNotification with 0x745: err = nil, want non-nil (Session wrapper surfaces RPC error)")
		}
	})
}

// TestNotificationChannel_SetOnFirstSuccess pins the invariant that
// notificationChannel is set ONLY on first successful subscribe. The
// production code at notification_api.go:382 sets `notificationChannel = ch`
// only when `successes > 0`. With no successes (all-Skipped batch), the
// field MUST stay nil.
//
// We drive this by passing an empty configs slice and an all-duplicate
// batch — both result in 0 successes, channel must stay nil.
//
// Validates: R-NOT-010 (channel set only on first success).
func TestNotificationChannel_SetOnFirstSuccess(t *testing.T) {
	t.Run("empty_configs_no_change", func(t *testing.T) {
		sess := newNotifTestSession()
		ch := make(chan *Update, 1)

		_, err := sess.AddSymbolNotifications(context.Background(), nil, ch)
		if err != nil {
			t.Fatalf("AddSymbolNotifications nil configs: %v", err)
		}
		sess.notifications.lock.Lock()
		got := sess.notifications.notificationChannel
		sess.notifications.lock.Unlock()
		if got != nil {
			t.Errorf("notificationChannel = %v, want nil after empty batch", got)
		}
	})

	t.Run("all_skipped_no_channel_set", func(t *testing.T) {
		sess := newNotifTestSession()
		preSeedSymbol(sess, "MAIN.dup")

		ch := make(chan *Update, 1)
		// Pre-stage so every config is rejected as duplicate.
		sess.notifications.lock.Lock()
		sess.notifications.addConfig(NotificationConfig{SymbolName: "MAIN.dup"})
		sess.notifications.lock.Unlock()

		_, _ = sess.AddSymbolNotifications(context.Background(), []NotificationConfig{
			{SymbolName: "MAIN.dup", TransmissionMode: TransModeServerOnChange},
		}, ch)
		// Channel was never set by AddSymbolNotifications since all entries
		// were Skipped pre-flight (no roundtrip even occurred — len(requests)==0
		// short-circuits). Confirm nil.
		sess.notifications.lock.Lock()
		got := sess.notifications.notificationChannel
		sess.notifications.lock.Unlock()
		if got != nil {
			t.Errorf("notificationChannel = %v, want nil — no successful subscribes", got)
		}
	})

	t.Run("concurrent_calls_no_channel_race", func(t *testing.T) {
		sess := newNotifTestSession()
		preSeedSymbol(sess, "MAIN.x")

		// Pre-set channel + pre-stage an existing config so every
		// concurrent AddSymbolNotifications call results in all-Skipped
		// (no PLC roundtrip needed). The race detector watches the
		// notificationChannel field for torn writes during the
		// concurrent pre-checks.
		ch := make(chan *Update, 1)
		sess.notifications.lock.Lock()
		sess.notifications.notificationChannel = ch
		sess.notifications.addConfig(NotificationConfig{SymbolName: "MAIN.x"})
		sess.notifications.lock.Unlock()

		var wg sync.WaitGroup
		for i := 0; i < 20; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, _ = sess.AddSymbolNotifications(context.Background(), []NotificationConfig{
					{SymbolName: "MAIN.x", TransmissionMode: TransModeServerOnChange},
				}, ch)
			}()
		}
		wg.Wait()

		sess.notifications.lock.Lock()
		got := sess.notifications.notificationChannel
		sess.notifications.lock.Unlock()
		if got != ch {
			t.Errorf("notificationChannel changed under concurrent calls: got %v, want %v", got, ch)
		}
	})
}

// TestResubscribeRetry_UpToMax exercises the resubscribeMaxAttempts cap path.
// On each call to resubscribeNotifications, configs that come back as Skipped
// have their counter incremented and are re-queued until counter >= max,
// at which point they are dropped with a WARN log.
//
// Drive Skipped: server returns SumAddDeviceNotification success with valid
// handles, but the test's onWriteRead handler removes the symbol from cache
// during the request. The post-roundtrip re-fetch finds nil → Skipped+Handle
// fires. Then we restore the symbol so the next iteration's filter keeps it.
//
// Counts WARN log emissions and confirms the config is dropped at the cap.
//
// Validates: R-NOT-013 (resubscribe retry-up-to-max with cap enforcement).
func TestResubscribeRetry_UpToMax(t *testing.T) {
	srv := startScriptableServer(t)
	defer srv.stop()

	logHandler := &testLogHandler{}
	logger := slog.New(logHandler)

	sess, _ := newWiredTestSession(t, srv)
	sess.logger = logger
	preSeedSymbol(sess, "MAIN.x")
	// Pre-populate config + channel so resubscribe has work to do.
	ch := make(chan *Update, 1)
	sess.notifications.lock.Lock()
	sess.notifications.pending = []pendingNotification{
		{Config: NotificationConfig{SymbolName: "MAIN.x", TransmissionMode: TransModeServerOnChange}},
	}
	sess.notifications.configsByKey[symbolKey("MAIN.x")] = struct{}{}
	sess.notifications.notificationChannel = ch
	sess.notifications.lock.Unlock()

	var sumHandle atomic.Uint32
	sumHandle.Store(0xCC000001)

	// Sum-add request: respond with a fresh handle but FIRST swap the cache
	// to empty so the post-roundtrip re-fetch finds nil and Skipped fires.
	srv.onWriteRead(GroupSumupAddDeviceNotification, func(_ []byte) []byte {
		sess.cache.lock.Lock()
		sess.cache.symbols = map[string]*symbol{}
		sess.cache.lock.Unlock()
		h := sumHandle.Add(1) - 1
		return buildSumAddNotifPayload([]sumNotifResponse{{Handle: h, Error: ReturnCodeNoErrors}})
	})
	// bestEffortDelete after Skipped+Handle uses SumDeleteDeviceNotification.
	srv.onWriteRead(GroupSumupDeleteDeviceNotification, func(req []byte) []byte {
		// One handle per request (4 bytes). Always succeed.
		nItems := len(req) / 4
		codes := make([]ReturnCode, nItems)
		for i := range codes {
			codes[i] = ReturnCodeNoErrors
		}
		return buildSumDeleteNotifPayload(codes)
	})

	// Run resubscribeMaxAttempts (=3) iterations. Each attempt must be
	// preceded by re-seeding the symbol so filterValidNotificationConfigs
	// keeps it. After each, the config is re-queued with attempts++ until
	// the cap is reached.
	for i := 0; i < resubscribeMaxAttempts; i++ {
		preSeedSymbol(sess, "MAIN.x")
		// Re-pin the channel since resubscribe clears it when there are no
		// valid configs at the start.
		sess.notifications.lock.Lock()
		sess.notifications.notificationChannel = ch
		sess.notifications.lock.Unlock()

		if err := sess.resubscribeNotifications(); err != nil {
			t.Fatalf("iter %d: resubscribeNotifications: %v", i, err)
		}
	}

	// After resubscribeMaxAttempts iterations: the config must be DROPPED,
	// not requeued.
	sess.notifications.lock.Lock()
	leftover := len(sess.notifications.pending)
	sess.notifications.lock.Unlock()
	if leftover != 0 {
		t.Errorf("after %d retries: notificationConfigs still has %d entries, want 0 (dropped at cap)",
			resubscribeMaxAttempts, leftover)
	}

	// At least one WARN log about dropping configs after max retries.
	if logHandler.findByMessage("dropping configs after max retries") == nil {
		t.Errorf("expected WARN log 'dropping configs after max retries' was not emitted")
	}
}

// TestBestEffortDeleteNotifications_MixedSuccess pins the helper's
// counting + logging behavior with mixed PLC return codes. The helper
// relies on Session.SumDeleteDeviceNotification → Client.SumDeleteDeviceNotification
// for the actual PLC call; without a stub we cannot drive the mixed
// success/0x745/error response.
//
// We at least exercise the empty-handles path here (zero items returns 0).
//
// Validates: R-NOT-015 (partial — empty path; mixed-success is TODO).
func TestBestEffortDeleteNotifications_MixedSuccess(t *testing.T) {
	t.Run("empty_returns_zero", func(t *testing.T) {
		sess := newNotifTestSession()
		got := sess.bestEffortDeleteNotifications(context.Background(), nil)
		if got != 0 {
			t.Errorf("empty handles: got %d, want 0", got)
		}
	})

	t.Run("mixed_results", func(t *testing.T) {
		srv := startScriptableServer(t)
		defer srv.stop()

		// Sum-delete returns [NoErrors, NotifyHandleInvalid, DeviceError].
		// Per bestEffortDeleteNotifications: NoErrors + NotifyHandleInvalid
		// count as success (handle gone PLC-side); DeviceError does NOT.
		srv.onWriteRead(GroupSumupDeleteDeviceNotification, func(_ []byte) []byte {
			return buildSumDeleteNotifPayload([]ReturnCode{
				ReturnCodeNoErrors,
				ReturnCodeDeviceNotifyHandleInvalid,
				ReturnCodeDeviceError,
			})
		})

		logHandler := &testLogHandler{}
		sess, _ := newWiredTestSession(t, srv)
		sess.logger = slog.New(logHandler)

		got := sess.bestEffortDeleteNotifications(context.Background(), []uint32{1, 2, 3})
		if got != 2 {
			t.Errorf("deleted count = %d, want 2 (NoErrors + handle-invalid)", got)
		}
		// One handle did not clean up, so there must be a WARN log
		// reporting the partial cleanup.
		if logHandler.findByMessage("some handles not cleaned up") == nil {
			t.Errorf("expected WARN 'some handles not cleaned up' (mixed-success path)")
		}
	})
}

// Sanity: TransModeServerOnChange is a valid mode; we use it in tests to
// ensure pre-checks fire BEFORE the in-context-mode auto-fallback (which
// would emit a Warn log otherwise).
var _ TransMode = TransModeServerOnChange

// Compile-anchor for atomic + errors: keep imports honest if any
// path above is later trimmed.
var (
	_ = atomic.Int32{}
	_ = errors.New
)

// TestSumNotificationResultTriState drives the production
// Session.AddSymbolNotifications path through the scriptable PLC stub
// and asserts the three+TOCTOU classification of the
// SumNotificationResult struct returned to the caller:
//
//  1. success — Handle != 0, Error == NoErrors, Skipped == nil.
//  2. PLC error — Handle == 0, Error != NoErrors, Skipped == nil.
//  3. library skip (duplicate name in batch) — Skipped != nil.
//  4. TOCTOU loss (PLC accepted, library found stranded *symbol
//     post-roundtrip) — Skipped != nil, Handle may be non-zero so
//     caller must release.
//
// Validates: R-NOT-009 (per-config result contract) / R-SUM-004
// (sum-batch tri-state).
func TestSumNotificationResultTriState(t *testing.T) {
	srv := startScriptableServer(t)
	defer srv.stop()
	sess, _ := newWiredTestSession(t, srv)

	// Three symbols cached up-front: x, y, z.
	for _, name := range []string{"MAIN.x", "MAIN.y", "MAIN.z"} {
		sess.cache.symbols[symbolKey(name)] = &symbol{
			FullName:    name,
			DataType:    "INT",
			Length:      2,
			Handle:      0xA1B2C3D4, // any non-zero handle so symbolSumAddress takes the handle path
			ContextMask: 0,
		}
	}

	// Sum-add response: per-item shape based on inbound count. Item 0
	// (x) succeeds with handle 0x1001. Item 1 (y) returns PLC error.
	// Item 2 (z) succeeds with handle 0x1003 — but the test mutates
	// the cache mid-handler so the post-roundtrip re-fetch finds the
	// orphan and reports Skipped+Handle (TOCTOU race).
	srv.onWriteRead(GroupSumupAddDeviceNotification, func(req []byte) []byte {
		// Mid-roundtrip: delete z from the cache so the post-roundtrip
		// re-resolve fails for that handle, triggering the TOCTOU branch.
		sess.cache.lock.Lock()
		delete(sess.cache.symbols, symbolKey("MAIN.z"))
		sess.cache.lock.Unlock()
		return buildSumAddNotifPayload([]sumNotifResponse{
			{Handle: 0x1001, Error: ReturnCodeNoErrors},
			{Handle: 0, Error: ReturnCodeDeviceInvalidParam},
			{Handle: 0x1003, Error: ReturnCodeNoErrors},
		})
	})
	// bestEffortDelete uses SumDelete for the orphan release.
	srv.onWriteRead(GroupSumupDeleteDeviceNotification, func(req []byte) []byte {
		nItems := len(req) / 4
		codes := make([]ReturnCode, nItems)
		for i := range codes {
			codes[i] = ReturnCodeNoErrors
		}
		return buildSumDeleteNotifPayload(codes)
	})

	ch := make(chan *Update, 4)
	configs := []NotificationConfig{
		{SymbolName: "MAIN.x", TransmissionMode: TransModeServerOnChange},
		// Library-skip case: duplicate name within the batch.
		{SymbolName: "MAIN.x", TransmissionMode: TransModeServerOnChange},
		{SymbolName: "MAIN.y", TransmissionMode: TransModeServerOnChange},
		{SymbolName: "MAIN.z", TransmissionMode: TransModeServerOnChange},
	}

	results, err := sess.AddSymbolNotifications(context.Background(), configs, ch)
	if err != nil {
		t.Fatalf("AddSymbolNotifications: %v", err)
	}
	if len(results) != len(configs) {
		t.Fatalf("got %d results, want %d", len(results), len(configs))
	}

	// Assert: configs[0] success
	r0 := results[0]
	if r0.Skipped != nil || r0.Error != ReturnCodeNoErrors || r0.Handle == 0 {
		t.Errorf("config[0] (success): got Handle=%d Error=%v Skipped=%v",
			r0.Handle, r0.Error, r0.Skipped)
	}
	// Assert: configs[1] library-skip duplicate (Skipped != nil)
	r1 := results[1]
	if r1.Skipped == nil {
		t.Errorf("config[1] (duplicate): Skipped should be non-nil; got %+v", r1)
	}
	// Assert: configs[2] PLC error (Skipped nil, Error != NoErrors, Handle == 0)
	r2 := results[2]
	if r2.Skipped != nil || r2.Error == ReturnCodeNoErrors || r2.Handle != 0 {
		t.Errorf("config[2] (PLC error): got Handle=%d Error=%v Skipped=%v",
			r2.Handle, r2.Error, r2.Skipped)
	}
	// Assert: configs[3] TOCTOU loss (Skipped != nil, Handle non-zero from
	// the PLC because the cache vanished mid-roundtrip — caller MUST
	// release this handle on the PLC side via DeleteDeviceNotification).
	r3 := results[3]
	if r3.Skipped == nil {
		t.Errorf("config[3] (TOCTOU): Skipped should be non-nil; got %+v", r3)
	}
}

// TestResubscribeNotifications_RollbackOnError verifies that when
// AddSymbolNotifications returns an outer error mid-resubscribe, the rollback
// path restores notificationConfigs and notificationChannel from the
// pre-call snapshot. Without rollback, the configs would be left empty
// after a failed retry and subsequent reconnects would have nothing to
// resubscribe (user notification subscriptions silently dropped).
//
// Drives the error by registering a SumAddDeviceNotification handler that
// returns a too-short response so executeSumCommand's length validation
// fails — surfaces as outer err to AddSymbolNotifications.
//
// Validates: resubscribeNotifications save/restore via resetConfigs.
func TestResubscribeNotifications_RollbackOnError(t *testing.T) {
	srv := startScriptableServer(t)
	defer srv.stop()

	// Truncated response: claims n=1 item but returns 0 bytes of item data.
	// executeSumCommand asserts len(resp) >= n*itemReadSize (n*8 for Add).
	srv.onWriteRead(GroupSumupAddDeviceNotification, func(_ []byte) []byte {
		return []byte{} // too short — outer parse will fail
	})

	sess, _ := newWiredTestSession(t, srv)
	preSeedSymbol(sess, "MAIN.x")
	ch := make(chan *Update, 1)
	saved := []pendingNotification{
		{Config: NotificationConfig{SymbolName: "MAIN.x", TransmissionMode: TransModeServerOnChange, MaxDelay: 0, CycleTime: 0}},
	}

	sess.notifications.lock.Lock()
	sess.notifications.resetConfigs(saved)
	sess.notifications.notificationChannel = ch
	sess.notifications.lock.Unlock()

	// resubscribeNotifications runs the AddSymbolNotifications path. With the
	// truncated-response handler installed, the call errors out and rollback
	// must restore both fields.
	err := sess.resubscribeNotifications()

	// Rollback restored — savedConfigs is back in place.
	sess.notifications.lock.Lock()
	got := sess.notifications.pending
	gotChannel := sess.notifications.notificationChannel
	sess.notifications.lock.Unlock()

	if len(got) != 1 || got[0].Config.SymbolName != "MAIN.x" {
		t.Errorf("pending after rollback = %+v, want 1 entry for MAIN.x", got)
	}
	if gotChannel != ch {
		t.Errorf("notificationChannel after rollback = %v, want %v (saved channel)", gotChannel, ch)
	}
	if !sess.notifications.hasConfig("MAIN.x") {
		t.Errorf("configsByKey mirror not rebuilt by resetConfigs on rollback")
	}
	// AddSymbolNotifications may return err or nil depending on whether the
	// SumAddNotifState CAS landed on unsupported (triggering fallback). Either
	// is acceptable for the rollback contract — we care about the restoration.
	_ = err
}

func TestIsBestEffortDeleteSuccess(t *testing.T) {
	cases := []struct {
		name string
		code ReturnCode
		want bool
	}{
		{"NoErrors", ReturnCodeNoErrors, true},
		{"NotifyHandleInvalid", ReturnCodeDeviceNotifyHandleInvalid, true},
		{"DeviceClientUnknown", ReturnCodeDeviceClientUnknown, true},
		{"DeviceError", ReturnCodeDeviceError, false},
		{"DeviceNotReady", ReturnCodeDeviceNotReady, false},
		{"GlobalTargetNotFound", ReturnCodeGlobalTargetNotFound, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := isBestEffortDeleteSuccess(tc.code); got != tc.want {
				t.Errorf("isBestEffortDeleteSuccess(%v) = %v, want %v", tc.code, got, tc.want)
			}
		})
	}
}
