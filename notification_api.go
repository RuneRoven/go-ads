package ads

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// notificationManager owns the connection-level notification state:
// the per-handle symbol map, the saved configs for reconnect re-subscribe,
// the user-supplied channel that all notifications are dispatched to, and
// the timestamp of the most recent successful subscribe (used to suppress
// "unknown handle" warnings during the first-sample race window).
//
// Lock ordering: NEVER hold both cache.lock and notifications.lock simultaneously.
// activeNotification couples a subscribed symbol with the user channel
// updates flow to. Stored in notificationManager.activeNotifications under
// notifications.lock. Pairing the channel with the symbol here (instead of
// on the symbol struct) keeps the cross-lock invariant entirely inside the
// notifications subsystem — symbol records reachable via cache.symbols no
// longer carry notifications.lock-guarded state.
type activeNotification struct {
	Sym *symbol
	Ch  chan<- *Update
}

type notificationManager struct {
	lock                sync.Mutex
	activeNotifications map[uint32]activeNotification
	// pending holds the resubscribe-aware copy of every active user
	// subscription. Internal type — exposed via NotificationConfig in public
	// API. configsByKey mirrors pending for O(1) duplicate-subscribe probes
	// (hot path under bulk Add). Keys are lower-cased symbol names to match
	// the EqualFold semantic used elsewhere. MUST be kept in lockstep with
	// pending — use addConfig / removeConfigByName / resetConfigs to mutate.
	pending             []pendingNotification
	configsByKey        map[string]struct{}
	notificationChannel chan *Update
	lastSubscribeNs     atomic.Int64
}

// addConfig wraps cfg into a fresh pendingNotification (resubscribeAttempts=0)
// and appends it to pending. Caller must hold lock.
func (m *notificationManager) addConfig(cfg NotificationConfig) {
	m.pending = append(m.pending, pendingNotification{Config: cfg})
	m.configsByKey[symbolKey(cfg.SymbolName)] = struct{}{}
}

// addPending appends an already-wrapped pending entry (preserving its
// resubscribeAttempts counter). Used by resubscribeNotifications when
// re-queueing Skipped retries. Caller must hold lock.
func (m *notificationManager) addPending(p pendingNotification) {
	m.pending = append(m.pending, p)
	m.configsByKey[symbolKey(p.Config.SymbolName)] = struct{}{}
}

// hasConfig returns true if any existing config matches symbolName
// (case-insensitive). Caller must hold lock.
func (m *notificationManager) hasConfig(symbolName string) bool {
	_, ok := m.configsByKey[symbolKey(symbolName)]
	return ok
}

// resetConfigs swaps the entire slice and rebuilds the key index. Used by
// resubscribeNotifications during the save/rollback dance. Caller must hold lock.
func (m *notificationManager) resetConfigs(p []pendingNotification) {
	m.pending = p
	m.configsByKey = make(map[string]struct{}, len(p))
	for _, entry := range p {
		m.configsByKey[symbolKey(entry.Config.SymbolName)] = struct{}{}
	}
}

// StaleInfo describes why a one-shot Stale Update was delivered. Non-nil iff
// the corresponding Update's value MAY be from a pre-online-change cache
// state. Reason is one of the documented Reason* constants.
type StaleInfo struct {
	Reason Reason
}

// Update is delivered to the user channel for each PLC notification sample.
// Stale is non-nil only on the first sample following a stale-cache detection
// (R-NOT-017 one-shot); the field is nil for normal samples. Couples the
// "this sample may be stale" signal with the reason in a single check —
// callers do `if u.Stale != nil { /* handle stale */ }`.
type Update struct {
	Variable  string
	Value     string
	TimeStamp time.Time
	Stale     *StaleInfo
}

// NotificationConfig holds configuration for a symbol notification, used for
// batch add and reconnect re-subscribe. MaxDelay and CycleTime use
// time.Duration for consistency with SumNotificationRequest and the rest of
// the standard library.
type NotificationConfig struct {
	SymbolName       string
	MaxDelay         time.Duration
	CycleTime        time.Duration
	TransmissionMode TransMode
}

// pendingNotification wraps a user-supplied NotificationConfig with internal
// resubscribe bookkeeping. resubscribeAttempts is incremented each time a
// reconnect re-subscribe round returns the config as Skipped (TOCTOU loss
// against a concurrent caller, cache stranded mid-batch). Above
// resubscribeMaxAttempts the library drops the entry rather than retrying
// forever.
type pendingNotification struct {
	Config              NotificationConfig
	resubscribeAttempts int
}

// resubscribeMaxAttempts caps Skipped-config retries across reconnect cycles
// to prevent infinite churn on persistently-flapping symbols. After the cap
// the config is dropped with a Warn log; the user must re-subscribe via
// AddSymbolNotification to re-establish.
const resubscribeMaxAttempts = 3

// AddSymbolNotification registers a notification for a single symbol.
// All notifications on one connection must share the same updateReceiver
// channel; subscribing the same symbol twice is rejected.
// On reconnect, the stored channel is used to re-subscribe all notifications.
// For multiple notifications, prefer AddSymbolNotifications.
//
// Channel ownership: the caller MUST NOT close updateReceiver while any
// notification is active on this connection. The library guards against
// accidental close with a recover (see deliverNotification), but a closed
// channel will silently drop notifications and emit Error logs. To stop
// receiving notifications, call DeleteDeviceNotification or Close() — these
// remove the PLC-side registration before the channel is no longer used.
func (sess *Session) AddSymbolNotification(ctx context.Context, symbolName string, maxDelay time.Duration, cycleTime time.Duration, transMode TransMode, updateReceiver chan *Update) (uint32, error) {
	// Pre-check: channel match + duplicate-subscribe.
	sess.notifications.lock.Lock()
	if sess.notifications.notificationChannel != nil && sess.notifications.notificationChannel != updateReceiver {
		sess.notifications.lock.Unlock()
		return 0, fmt.Errorf("all symbol notifications on a connection must use the same updateReceiver channel")
	}
	if sess.notifications.hasConfig(symbolName) {
		sess.notifications.lock.Unlock()
		return 0, fmt.Errorf("symbol %q already has an active notification; delete it before re-subscribing", symbolName)
	}
	sess.notifications.lock.Unlock()

	symbol, err := sess.getSymbol(ctx, symbolName)
	if err != nil {
		return 0, fmt.Errorf("notification for %q: %w", symbolName, err)
	}

	// Auto-fallback: InContext modes (5/6) require non-zero ContextMask on the symbol.
	// If ContextMask is 0 (single-task PLC, TC2, or variable not bound to a task),
	// downgrade to the regular mode (3/4) to avoid 0x070B errors or silent failures.
	actualMode := transMode
	if (transMode == TransModeServerCycle2 || transMode == TransModeServerOnChange2) && symbol.ContextMask == 0 {
		actualMode = downgradeTransMode(transMode)
		sess.logger.Warn("InContext mode not available for symbol (ContextMask=0), falling back",
			"symbol", symbolName,
			"requested", transMode.String(),
			"using", actualMode.String(),
			"flags", fmt.Sprintf("0x%04X", uint32(symbol.Flags)))
	}

	// Record subscribe time BEFORE the RPC so any first-sample arriving in the
	// race window between PLC handle return and our map insert sees a fresh
	// timestamp in handleNotification's unknown-handle log-level decision. If
	// stored after the RPC, the previous-subscribe timestamp would falsely
	// elevate a legitimate first-sample to Warn.
	sess.notifications.lastSubscribeNs.Store(time.Now().UnixNano())
	handle, err := sess.client.Load().AddDeviceNotification(ctx, 
		uint32(GroupSymbolValueByHandle),
		symbol.Handle,
		symbol.Length,
		actualMode,
		maxDelay,
		cycleTime)
	if err != nil {
		return 0, err
	}
	sess.logger.Info("notification created",
		"handle", handle,
		"symbol", symbolName,
		"mode", actualMode.String())
	// Re-fetch *symbol from cache before commit. The pointer obtained
	// pre-roundtrip may be orphaned if loadSymbols swapped the cache during
	// the network call; using the stranded pointer would write notifications
	// into a symbol no longer reachable via ReadFromSymbol. Take cache.lock
	// FIRST and release before taking notifications.lock (lock-ordering rule).
	// Capture epoch under the lock; re-check it under notifications.lock
	// (atomic Load is lock-free) to close the residual race window where a
	// THIRD loadSymbols could run between cache.lock release and notifications.lock
	// acquire and re-strand `fresh`.
	sess.cache.lock.Lock()
	fresh := sess.cache.symbols[symbolKey(symbolName)]
	cacheGen := sess.epoch()
	sess.cache.lock.Unlock()
	if fresh == nil {
		if delErr := sess.DeleteDeviceNotification(ctx, handle); delErr != nil {
			sess.logger.Warn("failed to release PLC handle after symbol vanished",
				"handle", handle, "symbol", symbolName, "error", delErr)
		}
		return 0, fmt.Errorf("symbol %q removed from cache during subscribe (likely online change or LoadSymbols)", symbolName)
	}
	// Re-check under notifications.lock to defend the channel/duplicate invariants
	// against concurrent callers that passed the pre-check while we were
	// doing the PLC roundtrip. On mismatch, release the just-acquired PLC handle.
	sess.notifications.lock.Lock()
	if sess.epoch() != cacheGen {
		sess.notifications.lock.Unlock()
		if delErr := sess.DeleteDeviceNotification(ctx, handle); delErr != nil {
			sess.logger.Warn("failed to release PLC handle after cache reload during subscribe",
				"handle", handle, "symbol", symbolName, "error", delErr)
		}
		return 0, fmt.Errorf("symbol %q stranded by concurrent cache reload during subscribe", symbolName)
	}
	if sess.notifications.notificationChannel != nil && sess.notifications.notificationChannel != updateReceiver {
		sess.notifications.lock.Unlock()
		if delErr := sess.DeleteDeviceNotification(ctx, handle); delErr != nil {
			sess.logger.Warn("failed to release PLC handle after channel-mismatch reject",
				"handle", handle, "symbol", symbolName, "error", delErr)
		}
		return 0, fmt.Errorf("all symbol notifications on a connection must use the same updateReceiver channel")
	}
	if sess.notifications.hasConfig(symbolName) {
		sess.notifications.lock.Unlock()
		if delErr := sess.DeleteDeviceNotification(ctx, handle); delErr != nil {
			sess.logger.Warn("failed to release PLC handle after duplicate-subscribe reject",
				"handle", handle, "symbol", symbolName, "error", delErr)
		}
		return 0, fmt.Errorf("symbol %q already has an active notification; delete it before re-subscribing", symbolName)
	}
	defer sess.notifications.lock.Unlock()
	sess.notifications.activeNotifications[handle] = activeNotification{Sym: fresh, Ch: updateReceiver}

	// Save config for reconnect re-subscribe
	sess.notifications.addConfig(NotificationConfig{
		SymbolName:       symbolName,
		MaxDelay:         maxDelay,
		CycleTime:        cycleTime,
		TransmissionMode: transMode,
	})
	sess.notifications.notificationChannel = updateReceiver

	return handle, nil
}

// AddSymbolNotifications adds multiple symbol notifications in a single ADS round-trip using SumAddDeviceNotification.
// Returns per-config results (parallel to configs) so callers can detect
// partial failure. A non-nil error indicates the batch could not be sent at
// all (transport failure); per-item state is still in the result slice.
//
// Per-result state:
//   - Skipped != nil: library refused to send (duplicate-subscribe, symbol
//     resolution failure, or transport-aborted batch). Error/Handle are not
//     meaningful.
//   - Skipped == nil && Error != ReturnCodeNoErrors: PLC accepted the batch
//     but rejected this item (e.g. invalid handle).
//   - Skipped == nil && Error == ReturnCodeNoErrors: success; Handle is valid.
//
// Channel ownership: the caller MUST NOT close ch while any notification is
// active on this connection. To stop receiving notifications, call
// DeleteDeviceNotification or Close() — these remove the PLC-side registration
// before the channel is no longer used.
func (sess *Session) AddSymbolNotifications(ctx context.Context, configs []NotificationConfig, ch chan *Update) ([]SumNotificationResult, error) {
	if len(configs) == 0 {
		return nil, nil
	}

	results := make([]SumNotificationResult, len(configs))

	// Snapshot already-subscribed symbol names so we can reject duplicates
	// pre-flight; the same check is repeated under the post-PLC lock to close
	// the TOCTOU window where a concurrent caller subscribed mid-roundtrip.
	sess.notifications.lock.Lock()
	if sess.notifications.notificationChannel != nil && sess.notifications.notificationChannel != ch {
		sess.notifications.lock.Unlock()
		return nil, fmt.Errorf("all symbol notifications on a connection must use the same updateReceiver channel")
	}
	// Snapshot the configsByKey mirror so the dup-check inside the batch loop
	// runs lock-free against a per-call copy (cheaper than re-acquiring the
	// manager lock for each candidate).
	existing := make(map[string]struct{}, len(sess.notifications.configsByKey))
	for k := range sess.notifications.configsByKey {
		existing[k] = struct{}{}
	}
	sess.notifications.lock.Unlock()

	// Resolve symbols and build requests; track which result index maps to
	// which request slot so we can splice the sum response back.
	type symbolInfo struct {
		configIndex int // index into configs/results
		config      NotificationConfig
		symbol      *symbol
	}
	var infos []symbolInfo
	var requests []SumNotificationRequest
	batchSeen := make(map[string]struct{}, len(configs))

	for i, cfg := range configs {
		key := symbolKey(cfg.SymbolName)
		if _, dup := existing[key]; dup {
			results[i].Skipped = fmt.Errorf("symbol %q already subscribed", cfg.SymbolName)
			sess.logger.Warn("duplicate notification rejected (already subscribed)", "symbol", cfg.SymbolName)
			continue
		}
		if _, dup := batchSeen[key]; dup {
			results[i].Skipped = fmt.Errorf("symbol %q duplicated within batch", cfg.SymbolName)
			sess.logger.Warn("duplicate notification rejected (within batch)", "symbol", cfg.SymbolName)
			continue
		}
		batchSeen[key] = struct{}{}

		symbol, err := sess.getSymbol(ctx, cfg.SymbolName)
		if err != nil {
			results[i].Skipped = fmt.Errorf("resolve symbol %q: %w", cfg.SymbolName, err)
			sess.logger.Error("error getting symbol for batch notification", "error", err, "symbol", cfg.SymbolName)
			continue
		}
		infos = append(infos, symbolInfo{configIndex: i, config: cfg, symbol: symbol})

		actualMode := cfg.TransmissionMode
		if (actualMode == TransModeServerCycle2 || actualMode == TransModeServerOnChange2) && symbol.ContextMask == 0 {
			actualMode = downgradeTransMode(actualMode)
			sess.logger.Warn("InContext mode not available for symbol (ContextMask=0), falling back",
				"symbol", cfg.SymbolName,
				"requested", cfg.TransmissionMode.String(),
				"using", actualMode.String(),
				"flags", fmt.Sprintf("0x%04X", uint32(symbol.Flags)))
		}

		requests = append(requests, SumNotificationRequest{
			Group:            uint32(GroupSymbolValueByHandle),
			Offset:           symbol.Handle,
			Length:           symbol.Length,
			TransmissionMode: actualMode,
			MaxDelay:         cfg.MaxDelay,
			CycleTime:        cfg.CycleTime,
		})
	}

	if len(requests) == 0 {
		return results, nil
	}

	// Record subscribe time BEFORE the batch RPC so first-sample arrivals
	// during PLC-roundtrip + map-insert race window log at Debug not Warn.
	// See AddSymbolNotification for the same ordering rationale.
	sess.notifications.lastSubscribeNs.Store(time.Now().UnixNano())
	subResults, err := sess.client.Load().SumAddDeviceNotification(ctx, requests)
	if err != nil {
		// Transport-aborted batch: every entry that was about to be sent must
		// be marked Skipped so callers can distinguish "lib didn't try" from
		// "PLC rejected".
		for _, info := range infos {
			results[info.configIndex].Skipped = fmt.Errorf("batch transport failure: %w", err)
		}
		return results, fmt.Errorf("batch add notification failed: %w", err)
	}

	// R-CACHE-009: fire online-change detection for first stale per-item code.
	// Once-per-batch semantics avoid callback amplification when multiple
	// items in the same response carry the same stale code (R-SES-011
	// "once per detection").
	for _, r := range subResults {
		if r.Error == ReturnCodeNoErrors {
			continue
		}
		if stale, _ := detectStaleCache(r.Error); stale {
			sess.handleStaleDetection(r.Error)
			break
		}
	}

	// Re-fetch *symbol pointers under cache.lock before commit. Defends
	// against a concurrent loadSymbols / online-change reload that swapped
	// cache.symbols while we did the PLC roundtrip - the originally-resolved
	// pointer would be orphaned (Handle=0, Value=""), and later
	// handleNotification would parse into the stranded symbol while
	// ReadFromSymbol would see a different fresh entry.
	// Capture epoch under the lock; re-check it under notifications.lock
	// to close the residual race where a third loadSymbols runs between
	// cache.lock release and notifications.lock acquire.
	freshSymbols := make([]*symbol, len(infos))
	sess.cache.lock.Lock()
	for i, info := range infos {
		freshSymbols[i] = sess.cache.symbols[symbolKey(info.config.SymbolName)]
	}
	cacheGen := sess.epoch()
	sess.cache.lock.Unlock()

	sess.notifications.lock.Lock()
	defer sess.notifications.lock.Unlock()

	// Generation re-check: if cache swapped between our cache.lock release
	// and now, every freshSymbols[] entry is potentially stranded - mark all
	// PLC-accepted entries as Skipped and surface handles for caller cleanup.
	if sess.epoch() != cacheGen {
		for i, r := range subResults {
			info := infos[i]
			if r.Error == ReturnCodeNoErrors {
				results[info.configIndex].Skipped = fmt.Errorf("cache reload during batch subscribe stranded symbol %q", info.config.SymbolName)
				results[info.configIndex].Handle = r.Handle
				sess.logger.Warn("batch entry stranded by cache reload; surfacing PLC handle for caller cleanup",
					"symbol", info.config.SymbolName, "handle", r.Handle)
			} else {
				results[info.configIndex] = r
			}
		}
		return results, nil
	}

	// TOCTOU re-check: another goroutine may have subscribed one of our names
	// while we were doing the PLC roundtrip. Mark such items Skipped and
	// surface handle so the caller can release the PLC-side registration.
	postExisting := make(map[string]struct{}, len(sess.notifications.configsByKey))
	for k := range sess.notifications.configsByKey {
		postExisting[k] = struct{}{}
	}

	successes := 0
	for i, r := range subResults {
		info := infos[i]
		key := symbolKey(info.config.SymbolName)
		if r.Error != ReturnCodeNoErrors {
			results[info.configIndex] = r
			sess.logger.Error("error adding notification in batch",
				"symbol", info.config.SymbolName,
				"errorCode", uint32(r.Error))
			continue
		}
		if _, dup := postExisting[key]; dup {
			// Concurrent subscribe won the race.
			results[info.configIndex].Skipped = fmt.Errorf("symbol %q subscribed concurrently during batch", info.config.SymbolName)
			results[info.configIndex].Handle = r.Handle
			sess.logger.Warn("batch entry lost TOCTOU race; surfacing PLC handle for caller cleanup",
				"symbol", info.config.SymbolName, "handle", r.Handle)
			continue
		}
		fresh := freshSymbols[i]
		if fresh == nil {
			// symbol vanished from cache between resolve and commit.
			results[info.configIndex].Skipped = fmt.Errorf("symbol %q removed from cache during batch (likely online change or LoadSymbols)", info.config.SymbolName)
			results[info.configIndex].Handle = r.Handle
			sess.logger.Warn("batch entry symbol vanished mid-flight; surfacing PLC handle for caller cleanup",
				"symbol", info.config.SymbolName, "handle", r.Handle)
			continue
		}
		results[info.configIndex] = r
		sess.notifications.activeNotifications[r.Handle] = activeNotification{Sym: fresh, Ch: ch}
		// addConfig wraps in a fresh pendingNotification with
		// resubscribeAttempts=0, so a successful subscribe naturally resets
		// any prior retry counter that may have been on this symbol.
		sess.notifications.addConfig(info.config)
		postExisting[key] = struct{}{}
		successes++
		sess.logger.Info("batch notification created",
			"handle", r.Handle,
			"symbol", info.config.SymbolName)
	}

	if successes > 0 {
		sess.notifications.notificationChannel = ch
	}

	return results, nil
}

// removeNotificationConfig removes the first config matching symbolName.
// Must be called with notifications.lock held.
func (sess *Session) removeNotificationConfig(symbolName string) {
	key := symbolKey(symbolName)
	if _, ok := sess.notifications.configsByKey[key]; !ok {
		return
	}
	delete(sess.notifications.configsByKey, key)
	for i, entry := range sess.notifications.pending {
		if strings.EqualFold(entry.Config.SymbolName, symbolName) {
			sess.notifications.pending = append(sess.notifications.pending[:i], sess.notifications.pending[i+1:]...)
			return
		}
	}
}
