package ads

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// notificationManager owns the connection-level notification state: the per-handle
// symbol map, the configs for reconnect re-subscribe, the user channel, and the
// last-subscribe timestamp that suppresses race-window warnings.
//
// Lock ordering: NEVER hold cache.lock and notifications.lock at once.
// activeNotification pairs the symbol with its channel here rather than on the
// symbol struct, keeping notifications.lock-guarded state out of cache.symbols.
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

	// lastSubscribeNs is the last time anything in the subscribe lifecycle
	// happened — a subscribe started, a subscribe finished, or a sweep refreshed
	// it. Deliberately all three: it drives only the short subscribeRaceWindow
	// tail that covers the gap after the last commit, and that tail should extend
	// past whichever of those happened most recently. It says nothing about what
	// is in flight; openSubscribes answers that.
	lastSubscribeNs atomic.Int64

	// openSubscribes maps each in-flight subscribe to the time it began, so every
	// one is bounded by subscribeRaceMaxOpen on its own age. Guarded by openMu,
	// which is also the critical section that closes a subscribe — one shared
	// counter plus one shared timestamp could not express this: the pair was not
	// atomic across goroutines (an ending subscribe could clear a clock a starting
	// one had just written, and "in flight with no clock" suppressed the orphan
	// reaper permanently), and a single slot recorded when the last quiet period
	// ended rather than when anything in flight began.
	//
	// Lock order where both are taken: openMu then earlyMu, never the reverse.
	openMu             sync.Mutex
	openSubscribes     map[subscribeToken]int64
	nextSubscribeToken uint64

	// subscribeInFlight mirrors len(openSubscribes) for lock-free reads on the
	// dispatch path and in tests. Written under openMu; authoritative answers
	// about the window come from newestOpenSubscribe. A subscribe is in flight
	// once it has issued the PLC-side Add and before it has committed every
	// handle into activeNotifications: during that gap an unknown handle is
	// presumed ours, so the sample is buffered (see earlySamples) and the orphan
	// reaper stays out.
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
	// earlyBytes is the total content held in earlySamples. The handle cap alone
	// does not bound memory: a sample is network-sized, and struct or array
	// symbols run to tens of KB, so 4096 of them is hundreds of MB decided
	// entirely by what the PLC sends for handles we do not recognise.
	earlyBytes int

	// heartbeatHandle is an internal CYCLIC notification on the symbol-version
	// index group. A subscription can die silently (TC3 across CONFIG->RUN: TCP up,
	// symbol version unchanged, no error, no more samples), and an on-change
	// subscription may be silent legitimately -- so only a cyclic beat's silence is
	// conclusive. 0xF008 dies with the runtime's notification table and its payload
	// is the symbol version, so each beat doubles as online-change detection.
	heartbeatHandle atomic.Uint32
	// heartbeatBeats counts delivered beats; the watchdog compares it against the
	// previous tick so the decision never touches a clock. A wall-clock step -- an
	// IPC with no RTC on its first NTP sync, a resumed VM -- otherwise reads as
	// elapsed time and kills every live subscription.
	heartbeatBeats atomic.Uint64
	// heartbeatEstablishFailures counts consecutive failures to register the beat,
	// so a PLC that refuses it permanently costs one Warn rather than one per retry.
	heartbeatEstablishFailures atomic.Int64
	// registered is what a healthy session holds, compared against
	// len(activeNotifications) to spot subscriptions that died unasked. Not
	// len(pending), which is a superset and outlives a handle. Raised by any commit;
	// lowered only by a caller teardown or a symbol the PLC no longer has.
	registered atomic.Int64
	// Samples for handles we do not own, and the deletes that follow, counted since
	// the last report. Reported on a timer rather than per episode: after a
	// reconnect orphans interleave with healthy samples, so a counter reset by the
	// next owned sample ends the episode at once and every orphan reports again.
	orphanSamples  atomic.Int64
	orphanWarnNs   atomic.Int64
	orphanDeletes  atomic.Int64
	orphanDeleteNs atomic.Int64
	// heartbeatLastNs is kept for the log line only ("silentFor"), never for the
	// decision. A stepped clock makes it a confusing number, not a wrong outcome.
	heartbeatLastNs atomic.Int64

	// resubscribeMu serialises whole re-subscribe sequences. Three paths run one --
	// reconnect, auto-reload after an online change, heartbeat recovery -- and each
	// snapshots pending and clears it, so two at once leaves one reading an empty
	// intent or both registering the same symbols twice on the PLC. Not `lock`,
	// which is the dispatch hot path's; never held while taking it beyond a snapshot.
	resubscribeMu sync.Mutex

	// orphanDelete tracks unknown-handle Delete attempts so we don't spam
	// the PLC when a previously-leaked subscription keeps firing. Guarded
	// by orphanMu (separate from notifications.lock so the throttle check
	// doesn't serialize against the dispatch hot path).
	orphanMu   sync.Mutex
	orphanSeen map[uint32]time.Time
	orphanSem  chan struct{} // bounded concurrency: cap = orphanDeleteMaxConcurrency
}

// subscribeToken identifies one in-flight subscribe. Returned by beginSubscribe
// and handed back to endSubscribe so each subscribe closes its own entry rather
// than sharing a counter with every other one.
type subscribeToken uint64

// earlySample is one buffered notification sample awaiting its handle's
// activeNotifications insert.
type earlySample struct {
	timestamp uint64
	content   []byte
}

// addConfig wraps cfg into a fresh pendingNotification (resubscribeAttempts=0)
// and files it under its symbol, so a successful subscribe naturally resets any
// prior retry counter. Caller must hold lock.
func (m *notificationManager) addConfig(cfg NotificationConfig) {
	m.setPending(pendingNotification{Config: cfg})
}

// addPending files an already-wrapped pending entry (preserving its
// resubscribeAttempts counter). Used by resubscribeNotifications when
// re-queueing Skipped retries. Caller must hold lock.
func (m *notificationManager) addPending(p pendingNotification) {
	m.setPending(p)
}

// setPending files one entry per symbol, REPLACING any entry already on file for
// that symbol rather than appending a second. Caller must hold lock.
//
// configsByKey cannot represent two entries for one symbol, and pending must not
// either: a duplicated entry there makes the next resubscribe register the symbol
// twice on the PLC, and the caller can only ever delete one of them. The case that
// reaches this with the key already present is a legal re-declaration of a symbol
// that is on file but not live — see hasLiveNotification.
func (m *notificationManager) setPending(p pendingNotification) {
	key := symbolKey(p.Config.SymbolName)
	if _, onFile := m.configsByKey[key]; onFile {
		for i := range m.pending {
			if symbolKey(m.pending[i].Config.SymbolName) == key {
				m.pending[i] = p
				return
			}
		}
	}
	m.pending = append(m.pending, p)
	m.configsByKey[key] = struct{}{}
}

// hasConfig returns true if any existing config matches symbolName
// (case-insensitive). Caller must hold lock.
//
// This answers "did the caller declare this symbol", NOT "is it subscribed" — use
// hasLiveNotification for the duplicate decision.
func (m *notificationManager) hasConfig(symbolName string) bool {
	_, ok := m.configsByKey[symbolKey(symbolName)]
	return ok
}

// hasLiveNotification reports whether symbolName has a committed handle. Caller
// must hold lock. This, not hasConfig, is what makes a subscribe a duplicate:
// pending is the caller's intent and outlives a handle, since a resubscribe
// re-queues retryable refusals that have no handle at all. Deciding on pending
// left such a symbol un-subscribable for the life of the session, with no exported
// way out because deletes work by handle.
func (m *notificationManager) subscriptionGap() (want, have int) {
	m.lock.Lock()
	defer m.lock.Unlock()
	return int(m.registered.Load()), len(m.activeNotifications)
}

// reportNow reports true at most once per interval, so a burst produces one line
// instead of one per event. Only the caller winning the CAS reports.
func reportNow(last *atomic.Int64, interval time.Duration) bool {
	now := time.Now().UnixNano()
	prev := last.Load()
	if now-prev < int64(interval) {
		return false
	}
	return last.CompareAndSwap(prev, now)
}

// orphanReportInterval bounds how often the orphan sample/delete lines appear.
const orphanReportInterval = 30 * time.Second

// raiseRegistered lifts the baseline to what is held now and never lowers it: a
// commit proves the session can hold that many, not that fewer is the new normal.
// Storing the count outright let a partial re-subscribe redefine healthy as the
// smaller set, leaving no gap for the symbols it failed to restore.
// Caller must hold lock.
func (m *notificationManager) raiseRegistered() {
	if n := int64(len(m.activeNotifications)); n > m.registered.Load() {
		m.registered.Store(n)
	}
}

// lowerRegisteredTo drops the baseline to n when it currently sits higher, for
// the cases where a healthy session genuinely holds less than it used to: a
// symbol the PLC no longer has, or a config abandoned after too many retries.
// Without it the gap check would chase handles that can never come back.
// Caller must hold lock.
func (m *notificationManager) lowerRegisteredTo(n int) {
	if int64(n) < m.registered.Load() {
		m.registered.Store(int64(n))
	}
}

func (m *notificationManager) hasLiveNotification(symbolName string) bool {
	key := symbolKey(symbolName)
	for _, entry := range m.activeNotifications {
		if entry.Sym != nil && symbolKey(entry.Sym.FullName) == key {
			return true
		}
	}
	return false
}

// liveNotificationNames snapshots the symbol keys that have a committed handle, so
// a batch can dup-check every candidate against a per-call copy instead of
// re-scanning under the manager lock per item. Caller must hold lock.
func (m *notificationManager) liveNotificationNames() map[string]struct{} {
	live := make(map[string]struct{}, len(m.activeNotifications))
	for _, entry := range m.activeNotifications {
		if entry.Sym != nil {
			live[symbolKey(entry.Sym.FullName)] = struct{}{}
		}
	}
	return live
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

// restoreConfigs puts a snapshot back WITHOUT discarding anything added since it
// was taken. Caller must hold lock.
//
// A recovery snapshots the caller's intent, tries, and puts the snapshot back if it
// failed. Doing that with resetConfigs is an overwrite: a subscribe that landed
// during the attempt is erased from the configs while its handle stays registered
// on the PLC, so it is never resubscribed after a reconnect and subscribing it
// again duplicates the registration. Anything already on file wins — it is newer
// than the snapshot by construction.
func (m *notificationManager) restoreConfigs(snapshot []pendingNotification) {
	for _, entry := range snapshot {
		if _, present := m.configsByKey[symbolKey(entry.Config.SymbolName)]; present {
			continue
		}
		m.addPending(entry)
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

// notificationReleaseTimeout bounds the cleanup delete a batch issues for
// handles it declined to bind, when the caller's own context is already done.
// Short on purpose: the caller has stopped waiting, and the handles die with the
// connection anyway if this does not land.
const notificationReleaseTimeout = 2 * time.Second

// releaseCleanupCtx picks the context for releasing handles the library declined
// to bind: a usable caller ctx as-is, a done one replaced, since the handles exist
// on the PLC either way. The replacement does NOT inherit cancellation -- the
// batch may have aborted because the session is closing -- so WithoutCancel keeps
// the values and a timeout bounds it. A nil lifecycle ctx falls back to Background.
func (sess *Session) releaseCleanupCtx(ctx context.Context) (context.Context, context.CancelFunc) {
	if ctx.Err() == nil {
		return ctx, func() {}
	}
	parent := context.Background()
	if sess.lifecycle != nil {
		if live := sess.currentLifecycleCtx(); live != nil {
			parent = context.WithoutCancel(live)
		}
	}
	return context.WithTimeout(parent, notificationReleaseTimeout)
}

// releaseUncommittedHandle gives back a handle acquired but never committed.
// Deliberately the raw Client call: the Session wrapper retires subscriptions the
// caller owns, and using it here cleared notificationChannel whenever
// activeNotifications was empty -- the state every sweep leaves behind. The
// release must also outlive a cancelled caller ctx, or a deadline expiring during
// the round-trip leaves the PLC streaming a subscription nobody owns.
func (sess *Session) releaseUncommittedHandle(ctx context.Context, handle uint32) error {
	releaseCtx, cancel := sess.releaseCleanupCtx(ctx)
	defer cancel()
	return sess.client.Load().DeleteDeviceNotification(releaseCtx, handle)
}

// takeNotificationHandles empties activeNotifications and returns every handle the
// PLC still holds, the internal heartbeat included. Collecting both together is the
// point: the heartbeat is deliberately not in activeNotifications, and the four
// callers that open-coded this mostly forgot it, leaving heartbeatHandle armed so
// establishHeartbeat became a no-op and the session ran on with no beat at all.
//
// quiesceDispatch bumps lastSubscribeNs so in-flight samples for the doomed handles
// read as race-window noise rather than Warn; a reconnect tearing the transport
// down does not need it. The heartbeat clock restarts rather than zeroing, since
// zero reads as "no beat expected yet" and would park the watcher.
func (sess *Session) takeNotificationHandles(quiesceDispatch bool) []uint32 {
	sess.notifications.lock.Lock()
	handles := make([]uint32, 0, len(sess.notifications.activeNotifications)+1)
	for h := range sess.notifications.activeNotifications {
		handles = append(handles, h)
	}
	if quiesceDispatch {
		sess.notifications.lastSubscribeNs.Store(time.Now().UnixNano())
	}
	sess.notifications.activeNotifications = make(map[uint32]activeNotification)
	sess.notifications.lock.Unlock()

	if hb := sess.notifications.heartbeatHandle.Swap(0); hb != 0 {
		handles = append(handles, hb)
	}
	sess.notifications.heartbeatLastNs.Store(time.Now().UnixNano())
	return handles
}

// releaseNotificationHandles best-effort deletes handles PLC-side and says how
// many the PLC confirmed. why names the caller in the log, since "requested=N
// deleted=M" is otherwise untraceable in a shutdown trace.
//
// Note when reading that log: bestEffortDelete counts 0x714/0x715 as
// success-equivalent, so deleted=N does not prove N registrations existed.
func (sess *Session) releaseNotificationHandles(ctx context.Context, handles []uint32, why string) int {
	if len(handles) == 0 {
		return 0
	}
	deleted := sess.bestEffortDeleteNotifications(ctx, handles)
	sess.logger.Info("released PLC notification handles", "why", why,
		"requested", len(handles), "deleted", deleted)
	return deleted
}

// AddSymbolNotification subscribes a single symbol. All notifications on one
// connection share the same channel, and a duplicate symbol is rejected; the
// stored channel is reused to re-subscribe after a reconnect. Prefer
// AddSymbolNotifications for more than one.
//
// The caller MUST NOT close updateReceiver while any notification is active -- a
// recover guards against it, but samples are silently dropped. Delete them or
// Close first.
func (sess *Session) AddSymbolNotification(ctx context.Context, symbolName string, maxDelay time.Duration, cycleTime time.Duration, transMode TransMode, updateReceiver chan *Update) (uint32, error) {
	// Refuse outside RUN rather than produce a misleading failure: in CONFIG the
	// runtime port does not exist, so this cannot succeed, and the PLC's answer is
	// an AMS "port not found" rather than anything about symbols. Permits when no
	// state has been observed — see requireRunningRuntime.
	if err := sess.requireRunningRuntime("AddSymbolNotification"); err != nil {
		return 0, err
	}
	// Pre-check: channel match + duplicate-subscribe.
	sess.notifications.lock.Lock()
	if sess.notifications.notificationChannel != nil && sess.notifications.notificationChannel != updateReceiver {
		sess.notifications.lock.Unlock()
		return 0, fmt.Errorf("all symbol notifications on a connection must use the same updateReceiver channel")
	}
	if sess.notifications.hasLiveNotification(symbolName) {
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
	subTok := sess.beginSubscribe()
	committed := make([]uint32, 0, 1)
	defer func() { sess.endSubscribe(ctx, subTok, committed) }()

	handle, err := sess.client.Load().AddDeviceNotification(ctx,
		uint32(GroupSymbolValueByHandle),
		symbol.Handle,
		symbol.Length,
		actualMode,
		maxDelay,
		cycleTime)
	if err != nil {
		// Online-change detection (R-CACHE-009). Same intercept as
		// readFromSymbolRetry / writeToSymbolRetry: the request above carried a
		// cached symbol handle, so a stale-cache code here means that handle is
		// dead. On TC3 a runtime restart answers 0x710 and does NOT bump the
		// symbol version, so this is the only signal there is — without it every
		// later subscribe repeats the same dead handle forever.
		//
		// Detection only, deliberately no retry: subscribe is not idempotent, and
		// a retry racing the reload's own resubscribe would double-register the
		// symbol. The caller's next call re-resolves by name and succeeds, which
		// is the same two-call shape the read path has in practice.
		var rc ReturnCode
		if errors.As(err, &rc) {
			sess.handleStaleDetection(rc)
		}
		return 0, err
	}
	// A subscription now exists, so it is worth protecting.
	_ = sess.establishHeartbeat(ctx)
	// Per-subscription, so it scales with the caller's symbol count. See the note
	// on "symbol resolved on-demand".
	sess.logger.Debug("notification created",
		"handle", handle,
		"symbol", symbolName,
		"mode", actualMode.String())
	// Subscribe time is when an unresolvable base type is actionable: the operator
	// can still load the datatype table before samples start arriving as
	// unconverted strings. Latched per symbol, so the read path stays quiet.
	sess.warnUnresolvedBaseType(symbolName)
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
		if delErr := sess.releaseUncommittedHandle(ctx, handle); delErr != nil {
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
		if delErr := sess.releaseUncommittedHandle(ctx, handle); delErr != nil {
			sess.logger.Warn("failed to release PLC handle after cache reload during subscribe",
				"handle", handle, "symbol", symbolName, "error", delErr)
		}
		return 0, fmt.Errorf("symbol %q stranded by concurrent cache reload during subscribe", symbolName)
	}
	if sess.notifications.notificationChannel != nil && sess.notifications.notificationChannel != updateReceiver {
		sess.notifications.lock.Unlock()
		if delErr := sess.releaseUncommittedHandle(ctx, handle); delErr != nil {
			sess.logger.Warn("failed to release PLC handle after channel-mismatch reject",
				"handle", handle, "symbol", symbolName, "error", delErr)
		}
		return 0, fmt.Errorf("all symbol notifications on a connection must use the same updateReceiver channel")
	}
	if sess.notifications.hasLiveNotification(symbolName) {
		sess.notifications.lock.Unlock()
		if delErr := sess.releaseUncommittedHandle(ctx, handle); delErr != nil {
			sess.logger.Warn("failed to release PLC handle after duplicate-subscribe reject",
				"handle", handle, "symbol", symbolName, "error", delErr)
		}
		return 0, fmt.Errorf("symbol %q already has an active notification; delete it before re-subscribing", symbolName)
	}
	defer sess.notifications.lock.Unlock()
	sess.notifications.activeNotifications[handle] = activeNotification{Sym: fresh, Ch: updateReceiver}
	// The PLC gave us this handle, so it is part of what a healthy session holds.
	sess.notifications.raiseRegistered()
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

// AddSymbolNotifications subscribes several symbols in one round-trip, returning
// per-config results parallel to configs. A non-nil error means the batch could
// not be sent at all; per-item state is still in the slice. Partial outcomes are
// normal -- a batch over a network is not atomic.
//
// Per result: Skipped != nil means the library did not commit it (match the
// ErrNotification* sentinels; a non-zero Handle there is a PLC registration
// released best-effort before returning). Otherwise Error carries the PLC's
// per-item verdict, and NoErrors means Handle is valid.
//
// The caller MUST NOT close ch while any notification is active; delete them or
// Close first.
func (sess *Session) AddSymbolNotifications(ctx context.Context, configs []NotificationConfig, ch chan *Update) ([]SumNotificationResult, error) {
	// Refuse outside RUN rather than produce a misleading failure: in CONFIG the
	// runtime port does not exist, so this cannot succeed, and the PLC's answer is
	// an AMS "port not found" rather than anything about symbols. Permits when no
	// state has been observed — see requireRunningRuntime.
	if err := sess.requireRunningRuntime("AddSymbolNotifications"); err != nil {
		return nil, err
	}
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
	// Snapshot the LIVE subscriptions so the dup-check inside the batch loop runs
	// lock-free against a per-call copy (cheaper than re-acquiring the manager lock
	// for each candidate). Live, not the configsByKey mirror: an entry on file with
	// no handle is a retry the caller may legitimately re-declare — see
	// hasLiveNotification.
	existing := sess.notifications.liveNotificationNames()
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
	subTok := sess.beginSubscribe()
	// Batch-scoped epoch. Compared per item, this refuses any commit once the
	// symbol cache has been reloaded since the batch began — which is what the
	// removed bulk re-check used to enforce. A per-item snapshot cannot: it only
	// sees a reload landing inside that one item's own commit.
	batchCacheEpoch := sess.epoch()
	committed := make([]uint32, 0, len(requests))
	committedIdx := make([]int, 0, len(requests))
	// Handles the PLC created but the library declined to bind. Released before
	// returning so a refusal cannot leak a subscription streaming to nobody.
	refused := make([]uint32, 0, len(requests))
	defer func() { sess.endSubscribe(ctx, subTok, committed) }()

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
			// Level by whether anyone has to act. A code in the stale-detection set
			// is the expected answer after a runtime restart: detection fires, the
			// handle is invalidated, and the next call re-resolves it. Measured on a
			// TC3 box, logging those at Error turned one ordinary restart into 22
			// ERROR lines in a second for a condition that healed immediately —
			// the log-flood shape this branch has already fixed twice elsewhere.
			// Anything else is something the library cannot resolve on its own.
			//
			// The code reaches the caller in results either way, so demoting costs
			// no signal.
			level := slog.LevelError
			if stale, _ := detectStaleCache(r.Error); stale {
				level = slog.LevelWarn
			}
			sess.logger.Log(context.Background(), level, "error adding notification in batch",
				"symbol", info.config.SymbolName,
				"errorCode", uint32(r.Error))
			return
		}
		if skipErr := sess.commitNotification(info.config, r.Handle, ch, batchCacheEpoch); skipErr != nil {
			results[info.configIndex].Skipped = skipErr
			results[info.configIndex].Handle = r.Handle
			// The PLC created this registration before we refused it, so it is
			// streaming to nobody. Release it after the batch rather than here:
			// on a sum-unsupported PLC this callback runs between individual
			// Adds, and a Delete wedged in there would serialize into the
			// registration sequence.
			refused = append(refused, r.Handle)
			sess.logger.Warn("batch entry not committed; releasing the PLC handle",
				"symbol", info.config.SymbolName, "handle", r.Handle, "reason", skipErr)
			return
		}
		results[info.configIndex] = r
		committed = append(committed, r.Handle)
		committedIdx = append(committedIdx, info.configIndex)
		// Deliver anything the PLC already sent for this handle before the bind
		// landed. Must be outside commitNotification — replay takes cache.lock.
		sess.replayEarlySamples(ctx, []uint32{r.Handle})
		// Per handle in the batch: subscribing 40 symbols produced 40 of these.
		sess.logger.Debug("batch notification created",
			"handle", r.Handle,
			"symbol", info.config.SymbolName)
		// See the note at the single-subscribe site.
		sess.warnUnresolvedBaseType(info.config.SymbolName)
	}

	// settle is the batch tail: the sweep amendment, then the release of every PLC
	// handle nothing on this side ended up owning. Deferred rather than called at
	// each return, because the bug it fixes was a return path that skipped it —
	// the transport-abort path, which is exactly where handles get created and not
	// bound. A defer covers every present and future exit, panics included. It is
	// registered AFTER the endSubscribe defer so LIFO runs settle first: the
	// amendment must decide what is stranded before the replay looks at what
	// committed. On a dead transport the delete short-circuits in
	// sumDeleteNotificationFallback, so this costs one failed round trip, not one
	// per handle.
	settle := func() {
		// A sweep wipes activeNotifications and deletes those handles PLC-side, so
		// anything this batch committed beforehand now exists on neither side and
		// must not be reported as success. Asked of the map, not a generation
		// counter: "was MINE swept" is per entry, and a counter would also strand an
		// item that commits after the sweep and is owned by nobody else.
		if len(committedIdx) > 0 {
			var stranded []int
			sess.notifications.lock.Lock()
			for _, idx := range committedIdx {
				if _, bound := sess.notifications.activeNotifications[results[idx].Handle]; !bound {
					stranded = append(stranded, idx)
					// commitNotification registered this symbol; un-committing it has
					// to unregister it too. The cleanup delete below cannot: it only
					// drops a config whose handle is still in activeNotifications, and
					// the sweep emptied that map. Left in place, the retry this entry's
					// error documents is rejected as a duplicate forever.
					sess.removeNotificationConfig(configs[idx].SymbolName)
				}
			}
			sess.notifications.lock.Unlock()

			for _, idx := range stranded {
				results[idx] = SumNotificationResult{
					Handle:  results[idx].Handle,
					Skipped: fmt.Errorf("symbol %q: %w", configs[idx].SymbolName, ErrNotificationStrandedByReload),
				}
			}
			if len(stranded) > 0 {
				sess.logger.Warn("notifications swept mid-batch; committed entries reported as stranded",
					"entries", len(stranded), "committed", len(committedIdx))
			}
		}

		// Release what we refused, plus anything the amendment just invalidated:
		// both are registrations on the PLC that nothing on this side owns.
		for _, idx := range committedIdx {
			if results[idx].Skipped != nil && results[idx].Handle != 0 {
				refused = append(refused, results[idx].Handle)
			}
		}
		if len(refused) > 0 {
			// A batch that aborted on a cancelled or expired ctx must still release
			// what the PLC created: reusing the dead ctx would fail the delete
			// before it was sent and leak a subscription streaming to nobody. The
			// session lifetime bounds the replacement, so a closing session does
			// not block here.
			releaseCtx, cancel := sess.releaseCleanupCtx(ctx)
			defer cancel()
			// Best-effort: 0x714 (already gone) counts as success, which covers the
			// stranded-by-reload case where the reload deleted them first.
			deleted := sess.bestEffortDeleteNotifications(releaseCtx, refused)
			sess.logger.Warn("released PLC notification handles the batch did not bind",
				"handles", len(refused), "deleted", deleted)
		}
	}
	defer settle()

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

	// Everything else was already bound (or recorded as failed/skipped) by
	// onItem as its result arrived; nothing left to commit here.
	if len(committed) > 0 {
		_ = sess.establishHeartbeat(ctx)
	}
	return results, nil
}

// commitNotification binds a PLC handle to its symbol so dispatchSample
// recognises it, returning the reason for any refusal so the caller can surface
// it as Skipped and release the registration. Called once per handle as soon as
// it is known, so a batch served one Add at a time binds progressively. Caller
// must hold neither lock: this takes cache.lock and releases it before the other.
func (sess *Session) commitNotification(cfg NotificationConfig, handle uint32, ch chan *Update, batchCacheEpoch uint64) error {
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
	if sess.epoch() != batchCacheEpoch {
		return fmt.Errorf("symbol %q: %w", cfg.SymbolName, ErrNotificationStrandedByReload)
	}
	if fresh == nil {
		return fmt.Errorf("symbol %q: %w", cfg.SymbolName, ErrNotificationSymbolVanished)
	}
	if sess.notifications.notificationChannel != nil && sess.notifications.notificationChannel != ch {
		return fmt.Errorf("symbol %q: %w", cfg.SymbolName, ErrNotificationChannelMismatch)
	}
	// activeNotifications already contains anything committed earlier in this same
	// batch (the insert below precedes addConfig), so it doubles as the in-batch
	// duplicate guard — no separate pre/post snapshot needed.
	if sess.notifications.hasLiveNotification(cfg.SymbolName) {
		return fmt.Errorf("symbol %q subscribed concurrently during batch: %w", cfg.SymbolName, ErrNotificationDuplicate)
	}

	sess.notifications.activeNotifications[handle] = activeNotification{Sym: fresh, Ch: ch}
	// Same as the single-subscribe site: this is what a healthy session holds.
	// Missing it here made the gap check inert for every batch subscriber, which
	// is how the plugin subscribes -- caught on hardware, want=0 have=1.
	sess.notifications.raiseRegistered()
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
