package ads

import (
	"context"
	"errors"
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

	// subscribeInFlight counts subscribe operations (single or batch) that
	// have issued the PLC-side Add but have not yet committed every handle
	// into activeNotifications. lastSubscribeNs alone cannot express this:
	// it records the last *started* subscribe, so a batch that takes longer
	// than subscribeRaceWindow to register (TC2 falls back to one Add per
	// symbol, so a 40-symbol batch takes hundreds of ms) leaves its own
	// early handles looking like leaked ones. While this counter is
	// non-zero an unknown handle is presumed ours: the sample is buffered
	// (see earlySamples) and the orphan reaper stays out.
	subscribeInFlight atomic.Int64

	// earlySamples holds samples that arrived for a handle before its
	// activeNotifications insert landed, keyed by handle. Guarded by
	// earlyMu (separate from lock so buffering never serializes against
	// the dispatch hot path). Only the most recent sample per handle is
	// kept — the point is not to replay history but to not lose the ONLY
	// sample a static symbol ever emits, and a live symbol's later samples
	// arrive on the normal path anyway.
	earlyMu      sync.Mutex
	earlySamples map[uint32]earlySample

	// orphanDelete tracks unknown-handle Delete attempts so we don't spam
	// the PLC when a previously-leaked subscription keeps firing. Guarded
	// by orphanMu (separate from notifications.lock so the throttle check
	// doesn't serialize against the dispatch hot path).
	orphanMu   sync.Mutex
	orphanSeen map[uint32]time.Time
	orphanSem  chan struct{} // bounded concurrency: cap = orphanDeleteMaxConcurrency
}

// earlySample is one buffered notification sample awaiting its handle's
// activeNotifications insert.
type earlySample struct {
	timestamp uint64
	content   []byte
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

// Reasons a batch subscribe refused to commit an entry, reported via
// SumNotificationResult.Skipped. They are sentinels because the right response
// differs per reason and callers should branch with errors.Is rather than on
// message text: a stranded or transport-failed entry is worth retrying, a
// duplicate or channel mismatch is a caller bug that retrying cannot fix.
//
// Each is wrapped with the symbol name, so errors.Is matches while the message
// still says which symbol.
var (
	// ErrNotificationDuplicate — the symbol already has an active notification.
	ErrNotificationDuplicate = errors.New("symbol already subscribed")
	// ErrNotificationChannelMismatch — all notifications on one connection must
	// share a single updateReceiver channel.
	ErrNotificationChannelMismatch = errors.New("all notifications on a connection must use the same updateReceiver channel")
	// ErrNotificationSymbolVanished — the symbol left the cache between resolve
	// and commit (online change, or a concurrent LoadSymbols).
	ErrNotificationSymbolVanished = errors.New("symbol removed from cache during subscribe")
	// ErrNotificationStrandedByReload — the symbol cache was reloaded while the
	// batch was in flight, so this entry's registration no longer exists.
	// Retryable: the caller (or resubscribeNotifications) should re-subscribe.
	ErrNotificationStrandedByReload = errors.New("symbol cache reloaded during batch subscribe")
	// ErrNotificationTransportFailure — the batch could not be sent.
	ErrNotificationTransportFailure = errors.New("batch transport failure")
)

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

	// Open the subscribe window BEFORE the RPC: the PLC-side handle exists as
	// soon as Add returns, so a first sample can arrive before the commit
	// below. endSubscribe replays anything buffered for the handle we commit
	// and closes the window. Deferred here so it runs after the
	// notifications.lock unlock defer registered further down (LIFO).
	sess.beginSubscribe()
	committed := make([]uint32, 0, 1)
	defer func() { sess.endSubscribe(ctx, committed) }()

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
	committed = append(committed, handle)

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
//   - Skipped != nil: the library did not commit this entry. Match it against
//     the ErrNotification* sentinels to decide what to do; ErrNotificationStranded
//     ByReload and ErrNotificationTransportFailure are worth retrying, the
//     others are caller bugs. Error is not meaningful. Handle IS meaningful
//     when non-zero: the PLC created that registration before the library
//     refused it, so the caller must release it (Session.DeleteDeviceNotification)
//     or it streams forever. resubscribeNotifications relies on this.
//   - Skipped == nil && Error != ReturnCodeNoErrors: PLC accepted the batch
//     but rejected this item (e.g. invalid handle).
//   - Skipped == nil && Error == ReturnCodeNoErrors: success; Handle is valid.
//
// Partial outcomes are normal: a batch over a network is not atomic. In
// particular a symbol-cache reload landing mid-batch invalidates entries this
// call had already committed, and those are reported as Skipped with
// ErrNotificationStrandedByReload rather than as successes.
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
			results[i].Skipped = fmt.Errorf("symbol %q: %w", cfg.SymbolName, ErrNotificationDuplicate)
			sess.logger.Warn("duplicate notification rejected (already subscribed)", "symbol", cfg.SymbolName)
			continue
		}
		if _, dup := batchSeen[key]; dup {
			results[i].Skipped = fmt.Errorf("symbol %q duplicated within batch: %w", cfg.SymbolName, ErrNotificationDuplicate)
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

	// Open the subscribe window BEFORE the batch RPC. This matters far more
	// here than in the single-symbol path: on a PLC without sum-command
	// support (TC2 answers 0x0701) SumAddDeviceNotification degrades to one
	// Add per symbol, so the earliest handles stream for the whole duration
	// of the remaining registrations. See AddSymbolNotification for the
	// defer-ordering rationale.
	sess.beginSubscribe()
	// Batch-scoped epoch. Compared per item, this refuses any commit once the
	// symbol cache has been reloaded since the batch began — which is what the
	// removed bulk re-check used to enforce. A per-item snapshot cannot: it only
	// sees a reload landing inside that one item's own commit.
	batchEpoch := sess.epoch()
	committed := make([]uint32, 0, len(requests))
	committedIdx := make([]int, 0, len(requests))
	defer func() { sess.endSubscribe(ctx, committed) }()

	// Bind each handle the moment its own Add returns. On a PLC that rejects
	// the sum command this runs between the individual Adds, so symbol #1 is
	// recognisable while symbol #40 is still being registered — instead of the
	// whole batch becoming recognisable at the end. Called synchronously on
	// this goroutine, so results/committed need no extra guarding.
	onItem := func(i int, r SumNotificationResult) {
		if i < 0 || i >= len(infos) {
			// Defensive: the index crosses a layer boundary. One bad index would
			// otherwise panic inside the RPC call.
			sess.logger.Error("notification batch callback got an out-of-range index",
				"index", i, "items", len(infos))
			return
		}
		info := infos[i]
		if r.Skipped != nil {
			// The Client layer already refused this one — e.g. the batch was
			// abandoned after the transport failed. Never treat it as bindable:
			// its Handle is zero and committing it would put a zero handle in
			// activeNotifications.
			results[info.configIndex] = r
			return
		}
		if r.Handle == 0 && r.Error == ReturnCodeNoErrors {
			results[info.configIndex].Skipped = fmt.Errorf("symbol %q: PLC reported success without a handle", info.config.SymbolName)
			sess.logger.Error("notification batch: success with a zero handle",
				"symbol", info.config.SymbolName)
			return
		}
		if r.Error != ReturnCodeNoErrors {
			results[info.configIndex] = r
			sess.logger.Error("error adding notification in batch",
				"symbol", info.config.SymbolName,
				"errorCode", uint32(r.Error))
			return
		}
		if skipErr := sess.commitNotification(info.config, r.Handle, ch, batchEpoch); skipErr != nil {
			results[info.configIndex].Skipped = skipErr
			results[info.configIndex].Handle = r.Handle
			sess.logger.Warn("batch entry not committed; surfacing PLC handle for caller cleanup",
				"symbol", info.config.SymbolName, "handle", r.Handle, "reason", skipErr)
			return
		}
		results[info.configIndex] = r
		committed = append(committed, r.Handle)
		committedIdx = append(committedIdx, info.configIndex)
		// Deliver anything the PLC already sent for this handle before the bind
		// landed. Must be outside commitNotification — replay takes cache.lock.
		sess.replayEarlySamples(ctx, []uint32{r.Handle})
		sess.logger.Info("batch notification created",
			"handle", r.Handle,
			"symbol", info.config.SymbolName)
	}

	subResults, err := sess.client.Load().sumAddDeviceNotificationFunc(ctx, requests, onItem)
	if err != nil {
		// Transport-aborted batch: every entry that was about to be sent must
		// be marked Skipped so callers can distinguish "lib didn't try" from
		// "PLC rejected". Anything onItem already reported keeps its own, more
		// specific outcome — tested via Skipped rather than the Error/Handle
		// pair, which also matches entries onItem refused (those carry a zero
		// Error and a live Handle).
		for _, info := range infos {
			r := results[info.configIndex]
			reported := r.Skipped != nil || r.Handle != 0 || r.Error != ReturnCodeNoErrors
			if reported {
				continue
			}
			results[info.configIndex].Skipped = fmt.Errorf("%w: %w", ErrNotificationTransportFailure, err)
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

	// Retroactive amendment. autoReloadOnStaleDetection bumps the epoch BEFORE
	// it snapshots activeNotifications and swaps the map, so anything this batch
	// committed successfully was in that snapshot and has since been deleted
	// PLC-side by the reload. Reporting those as successes would hand the caller
	// subscriptions that exist on neither side, silently. No unwinding is needed
	// precisely because the reload already released them.
	if len(committedIdx) > 0 && sess.epoch() != batchEpoch {
		for _, idx := range committedIdx {
			results[idx] = SumNotificationResult{
				Handle:  results[idx].Handle,
				Skipped: fmt.Errorf("symbol %q: %w", configs[idx].SymbolName, ErrNotificationStrandedByReload),
			}
		}
		sess.logger.Warn("symbol cache reloaded mid-batch; committed entries reported as stranded",
			"entries", len(committedIdx))
	}

	// Everything else was already bound (or recorded as failed/skipped) by
	// onItem as its result arrived; nothing left to commit here.
	return results, nil
}

// commitNotification binds a PLC-assigned handle to its symbol, making the
// handle recognisable to dispatchSample. Returns nil on success, or the reason
// the library refused to commit — the caller surfaces that as Skipped together
// with the handle so the PLC-side registration can be released.
//
// Called once per handle, as soon as that handle is known, so a batch that the
// PLC serves one Add at a time has each symbol bound while the rest are still
// registering rather than all of them at the end.
//
// Caller must hold neither cache.lock nor notifications.lock: this takes
// cache.lock first and releases it before taking notifications.lock, per the
// never-both-held rule.
func (sess *Session) commitNotification(cfg NotificationConfig, handle uint32, ch chan *Update, batchEpoch uint64) error {
	// Re-fetch the *symbol under cache.lock. The pointer resolved before the
	// PLC round-trip may have been stranded by a concurrent loadSymbols /
	// online-change reload that swapped cache.symbols.
	sess.cache.lock.Lock()
	fresh := sess.cache.symbols[symbolKey(cfg.SymbolName)]
	sess.cache.lock.Unlock()

	sess.notifications.lock.Lock()
	defer sess.notifications.lock.Unlock()

	// Compare against the epoch the BATCH started with, not one snapshotted
	// moments ago: a reload that landed earlier in this batch must invalidate
	// every remaining item, not just the one unlucky enough to straddle it.
	// Unchanged epoch also means the `fresh` pointer read above is current.
	if sess.epoch() != batchEpoch {
		return fmt.Errorf("symbol %q: %w", cfg.SymbolName, ErrNotificationStrandedByReload)
	}
	if fresh == nil {
		return fmt.Errorf("symbol %q: %w", cfg.SymbolName, ErrNotificationSymbolVanished)
	}
	if sess.notifications.notificationChannel != nil && sess.notifications.notificationChannel != ch {
		return fmt.Errorf("symbol %q: %w", cfg.SymbolName, ErrNotificationChannelMismatch)
	}
	// configsByKey is authoritative and already contains anything committed
	// earlier in this same batch, so it doubles as the in-batch duplicate
	// guard — no separate pre/post snapshot needed.
	if sess.notifications.hasConfig(cfg.SymbolName) {
		return fmt.Errorf("symbol %q subscribed concurrently during batch: %w", cfg.SymbolName, ErrNotificationDuplicate)
	}

	sess.notifications.activeNotifications[handle] = activeNotification{Sym: fresh, Ch: ch}
	// addConfig wraps in a fresh pendingNotification with resubscribeAttempts=0,
	// so a successful subscribe naturally resets any prior retry counter.
	sess.notifications.addConfig(cfg)
	sess.notifications.notificationChannel = ch
	return nil
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
