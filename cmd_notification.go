package ads

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"time"
)

// Single-symbol device-notification raw RPCs on *Client:
// AddDeviceNotification, DeleteDeviceNotification. Notification persistence
// and activeNotifications cleanup is the Session's wrapper concern (see
// Session.DeleteDeviceNotification below). The cache-aware
// handleNotification dispatcher also lives in this file because it shares
// Update / handle bookkeeping; it is wired into the Client via
// Client.SetNotificationHandler at Session.Connect and Session.dialAndStart
// (the two Client-allocation sites in session.go).

// durationToADSTicks converts a time.Duration to ADS 100ns tick units (uint32).
// Returns an error if d is negative or exceeds the ADS 32-bit limit (~429.5 s).
func durationToADSTicks(d time.Duration, name string) (uint32, error) {
	if d < 0 {
		return 0, fmt.Errorf("%s must be non-negative, got %v", name, d)
	}
	ticks := d.Nanoseconds() / 100
	if ticks > math.MaxUint32 {
		return 0, fmt.Errorf("%s exceeds ADS 32-bit 100ns limit (~429.5s), got %v", name, d)
	}
	return uint32(ticks), nil
}

// AddDeviceNotification registers a device notification with the PLC and
// returns the PLC-assigned handle. Raw RPC: no Session-side persistence.
// Callers wanting auto-resubscribe-on-reconnect use Session.AddSymbolNotification.
func (c *Client) AddDeviceNotification(
	ctx context.Context,
	group uint32,
	offset uint32,
	length uint32,
	transmissionMode TransMode,
	maxDelay time.Duration,
	cycleTime time.Duration,
) (handle uint32, err error) {
	request := new(bytes.Buffer)
	type addDeviceNotificationCommandPacket struct {
		Group            uint32
		Offset           uint32
		Length           uint32
		TransmissionMode uint32
		MaxDelay         uint32
		CycleTime        uint32
		Reserved         [16]byte
	}
	maxDelayTicks, err := durationToADSTicks(maxDelay, "maxDelay")
	if err != nil {
		return 0, err
	}
	cycleTimeTicks, err := durationToADSTicks(cycleTime, "cycleTime")
	if err != nil {
		return 0, err
	}
	content := addDeviceNotificationCommandPacket{
		group,
		offset,
		length,
		uint32(transmissionMode),
		maxDelayTicks,
		cycleTimeTicks,
		[16]byte{},
	}
	if err = binary.Write(request, binary.LittleEndian, content); err != nil {
		return 0, fmt.Errorf("binary.Write failed: %w", err)
	}
	type addDeviceNotificationResponse struct {
		Error  ReturnCode
		Handle uint32
	}
	resp, err := c.sendRequest(ctx, CommandIDAddDeviceNotification, request.Bytes())
	if err != nil {
		return
	}
	respBuffer := bytes.NewBuffer(resp)
	notificationResponse := addDeviceNotificationResponse{}
	if err = binary.Read(respBuffer, binary.LittleEndian, &notificationResponse); err != nil {
		// Error stays: a response that will not decode is broken framing, not a PLC
		// saying no. Retrying cannot fix it and someone has to look.
		c.logger.Error("failed to parse notification response", "error", err)
		return 0, err
	}
	if notificationResponse.Error != 0 {
		// Warn, not Error: the PLC refusing an Add is the normal answer while the
		// runtime is in CONFIG, and the heartbeat watcher retries on a cadence — an
		// Error per attempt is what produced 28% of one integration log. The code is
		// returned to the caller either way, so nothing loses its signal.
		c.logger.Warn("failed to add notification handler", "errorCode", uint32(notificationResponse.Error))
		return 0, fmt.Errorf("unable to create notification: %w", notificationResponse.Error)
	}
	c.logger.Log(context.Background(), LevelTrace, "added notification handler", "handle", notificationResponse.Handle)
	return notificationResponse.Handle, nil
}

// DeleteDeviceNotification deletes a device notification by handle. Raw RPC:
// returns the wire-level success/error. Callers that maintain
// activeNotifications must clean up themselves (Session does this in its
// wrapper Session.DeleteDeviceNotification below).
func (c *Client) DeleteDeviceNotification(ctx context.Context, handle uint32) error {
	request := &bytes.Buffer{}
	type deleteNotificationCommandPacket struct {
		Handle uint32
	}
	content := deleteNotificationCommandPacket{handle}
	if err := binary.Write(request, binary.LittleEndian, content); err != nil {
		return fmt.Errorf("binary.Write failed: %w", err)
	}
	resp, err := c.sendRequest(ctx, CommandIDDeleteDeviceNotification, request.Bytes())
	if err != nil {
		c.logger.Warn("error deleting handle", "handle", handle, "error", err)
		return err
	}
	respBuffer := bytes.NewBuffer(resp)
	var adsError ReturnCode
	if err = binary.Read(respBuffer, binary.LittleEndian, &adsError); err != nil {
		return fmt.Errorf("failed to parse DeleteDeviceNotification response: %w", err)
	}
	if adsError > 0 {
		c.logger.Warn("error deleting handle", "handle", handle, "errorCode", uint32(adsError))
		return fmt.Errorf("ADS error in DeleteDeviceNotification: %w", adsError)
	}
	// Debug, not Info: at this layer there is no symbol name to report, and
	// teardown of an N-symbol subscription would print N unhelpful lines. The
	// Session wrappers log the named line and the summary.
	c.logger.Debug("deleted notification handle", "handle", handle)
	return nil
}

// DeleteDeviceNotification on Session wraps the raw Client RPC with
// notifications.lock cleanup: removes the entry from activeNotifications, drops
// the cached notificationConfig, and clears notificationChannel when the
// last subscription dies. Callers that want raw delete behavior use
// the Client method directly.
func (sess *Session) DeleteDeviceNotification(ctx context.Context, handle uint32) error {
	// Snapshot symbol name BEFORE PLC RPC so a concurrent Reconnect clearing
	// activeNotifications mid-flight doesn't strand notificationConfigs (which
	// would cause resubscribeNotifications to re-subscribe a deleted symbol).
	sess.notifications.lock.Lock()
	var symbolName string
	entry, ours := sess.notifications.activeNotifications[handle]
	if ours && entry.Sym != nil {
		symbolName = entry.Sym.FullName
	}
	sess.notifications.lock.Unlock()

	err := sess.client.Load().DeleteDeviceNotification(ctx, handle)
	// 0x714 NotifyHandleInvalid and 0x715 DeviceClientUnknown mean the PLC has no
	// such registration — after a runtime restart or a dropped client identity that
	// is the normal answer, and the batch path has always counted them as
	// success-equivalent. Returning early on them stranded the entry forever (every
	// retry gets the same code) and left the config on file, so the next reconnect
	// re-subscribed a symbol the caller had deleted. Report the error, but clean up.
	if err != nil && !isBestEffortDeleteSuccessErr(err) {
		return err
	}
	sess.notifications.lock.Lock()
	if symbolName != "" {
		sess.removeNotificationConfig(symbolName)
	}
	delete(sess.notifications.activeNotifications, handle)
	// Gated on the handle having actually been ours, not merely on the map being
	// empty. A raw-handle caller — or one of AddSymbolNotification's own refusal
	// paths releasing a handle it never committed — would otherwise clear the
	// channel whenever the map happened to be empty, which is exactly the state a
	// sweep leaves behind. resubscribeNotifications then returns early on the nil
	// channel while the reconnect reports success, and every subscription is
	// silently dropped.
	if ours && len(sess.notifications.activeNotifications) == 0 {
		sess.notifications.notificationChannel = nil
	}
	sess.notifications.lock.Unlock()
	// Name the symbol: a handle alone cannot be tied back to a tag when
	// reading a shutdown trace, and this layer is the only one that knows the
	// mapping. Empty when the handle was not ours (raw-handle caller).
	if err != nil {
		// Cleaned up, but say what the PLC actually answered rather than reporting
		// a clean delete.
		sess.logger.Info("notification bookkeeping cleared; the PLC had already dropped the registration",
			"handle", handle, "symbol", symbolName, "error", err)
		return err
	}
	sess.logger.Info("notification deleted", "handle", handle, "symbol", symbolName)
	return nil
}

// SumDeleteDeviceNotification on Session wraps the raw Client RPC with
// notifications.lock cleanup. Returns the per-handle ReturnCode slice from the
// PLC. Successfully deleted handles (or handle-invalid / client-unknown,
// treated as success-equivalent per isBestEffortDeleteSuccess) are removed
// from activeNotifications.
//
// When the underlying Client returns a partial-codes-plus-error result —
// e.g., the per-handle fallback short-circuits on transport failure mid-
// batch — the codes processed so far ARE still flushed from
// activeNotifications, and both the partial slice and the error are
// surfaced to the caller. This keeps in-memory state coherent with what
// the PLC actually saw, rather than leaving phantom entries that would
// trigger duplicate-cleanup loops on the next reconnect.
func (sess *Session) SumDeleteDeviceNotification(ctx context.Context, handles []uint32) ([]ReturnCode, error) {
	return sess.sumDeleteDeviceNotification(ctx, handles, true)
}

// sumDeleteDeviceNotification is SumDeleteDeviceNotification with control over the
// "last subscription died" bookkeeping.
//
// userTeardown=false is for internal cleanup — the reconnect and reload paths,
// which delete PLC-side registrations they intend to recreate immediately. Those
// paths wipe activeNotifications BEFORE deleting, so the empty-map rule below
// would fire on every reconnect, clear notificationChannel, and make
// resubscribeNotifications return early on a nil channel: reconnect reported
// success, the FSM said Connected, and not one notification ever came back.
// Found by power-cycling a TC2 (see TestReconnect_CleanupKeepsTheUserChannel);
// no stub test had caught it because none asserted resumption after a cleanup.
func (sess *Session) sumDeleteDeviceNotification(ctx context.Context, handles []uint32, userTeardown bool) ([]ReturnCode, error) {
	codes, rpcErr := sess.client.Load().SumDeleteDeviceNotification(ctx, handles)
	if len(codes) == 0 {
		return codes, rpcErr
	}
	sess.notifications.lock.Lock()
	// Flush only handles for which we have a code. On partial-codes-plus-
	// error, len(codes) < len(handles) — leave the untried handles in
	// activeNotifications; the caller's retry / reconnect path picks them
	// up. min() guards against any future signature change where codes
	// might exceed handles.
	limit := len(codes)
	if len(handles) < limit {
		limit = len(handles)
	}
	deleted := 0
	for i := 0; i < limit; i++ {
		if !isBestEffortDeleteSuccess(codes[i]) {
			continue
		}
		h := handles[i]
		// Resolve the name before dropping the entry — this is the only place
		// it is still known, and a handle with no symbol is not traceable back
		// to a tag by the consumer (NotifyResult carries no handle).
		symbolName := ""
		if entry, ok := sess.notifications.activeNotifications[h]; ok && entry.Sym != nil {
			symbolName = entry.Sym.FullName
			sess.removeNotificationConfig(symbolName)
		}
		delete(sess.notifications.activeNotifications, h)
		deleted++
		// Debug per handle: routine teardown of an N-symbol subscription is not
		// worth N Info lines. The summary below is the Info-worthy event.
		sess.logger.Debug("batch deleted notification handle",
			"handle", h, "symbol", symbolName, "errorCode", uint32(codes[i]))
	}
	// Only a user deleting their last subscription releases the channel, so a
	// later Add may bring a different one. Internal cleanup is mid-recovery: the
	// same channel is about to be reused by the resubscribe.
	if userTeardown && len(sess.notifications.activeNotifications) == 0 {
		sess.notifications.notificationChannel = nil
	}
	remaining := len(sess.notifications.activeNotifications)
	sess.notifications.lock.Unlock()
	if deleted > 0 {
		sess.logger.Info("notifications deleted",
			"deleted", deleted, "requested", len(handles), "remaining", remaining)
	}
	return codes, rpcErr
}

const (
	windowsTick    int64 = 10000000
	secToUnixEpoch int64 = 11644473600
)

// NotificationStream is the outer header of an ADS DeviceNotification
// payload: Length is the total payload length in bytes, Stamps is the
// number of StampHeader records that follow.
type NotificationStream struct {
	Length uint32
	Stamps uint32
}

// StampHeader prefixes the samples that share a single PLC sample
// timestamp. Timestamp is in Windows FILETIME ticks (100 ns since 1601-01-01
// UTC). Samples is the count of NotificationSample records that follow.
type StampHeader struct {
	Timestamp uint64
	Samples   uint32
}

// NotificationSample is the per-handle prefix inside a stamp record:
// Handle identifies the subscription that produced the value; Size is the
// byte length of the value bytes that follow.
type NotificationSample struct {
	Handle uint32
	Size   uint32
}

// DeviceNotification (ADS cmd 8) packet decoder lives on *Client — see
// Client.deviceNotification. Session.handleNotification below is the
// cache-aware handler installed via Client.SetNotificationHandler from
// Session.Connect.

func (sess *Session) handleNotification(ctx context.Context, handle uint32, timestamp uint64, content []byte) {
	sess.dispatchSample(ctx, handle, timestamp, content, true)
}

// dispatchSample is the body of handleNotification. buffer controls whether an
// unknown handle may be parked in earlySamples: true on the live path, false
// when replayEarlySamples is re-dispatching a buffered sample, so a handle
// whose subscribe never committed cannot bounce between buffer and replay.
func (sess *Session) dispatchSample(ctx context.Context, handle uint32, timestamp uint64, content []byte, buffer bool) {
	// The heartbeat is ours, not the caller's: consume it before the
	// unknown-handle path can mistake it for a leaked subscription.
	if sess.consumeHeartbeat(handle, content) {
		return
	}
	// notifications.lock: handle lookup + symbol pointer/channel snapshot.
	sess.notifications.lock.Lock()
	entry, ok := sess.notifications.activeNotifications[handle]
	if !ok {
		sess.notifications.lock.Unlock()
		// Stale notifications are expected during:
		// - Close(): handles deleted from activeNotifications while listen() still drains
		// - Reconnect: Session.Reconnect clears activeNotifications before new subscriptions
		// - first-sample race — the PLC fires the first notification before our
		//   activeNotifications insert completes. The PLC-side handle exists the
		//   moment Add returns, so this window is unavoidable; it is wide enough
		//   to matter whenever a subscribe is still in flight.
		switch {
		case sess.isClosed() || sess.isReconnecting():
			sess.logger.Debug("received notification for deleted handle (expected during close/reconnect)", "handle", handle)
		case buffer && sess.subscribeRaceActive():
			// Our own subscribe is mid-flight, so this handle is almost
			// certainly one we are about to commit. Park the sample instead of
			// dropping it: a static symbol (constant string/bool) emits exactly
			// one sample, at subscribe time, and dropping it means the consumer
			// never sees that tag at all.
			sess.bufferEarlySample(ctx, handle, timestamp, content)
		default:
			// Genuine orphan sample — handle is registered on the PLC but
			// not in our client-side map. Most likely cause: a prior process
			// (us or another go-ads client with same source NetID+port) left
			// the subscription behind on crash/restart. Schedule a Delete
			// on the PLC so the orphan handle table slot is freed; without
			// this cleanup the TwinCAT AMS router accumulates entries
			// across restarts until it crashes (Beckhoff issue #268).
			sess.logger.Warn("received notification for unknown handle", "handle", handle)
			sess.tryOrphanDelete(handle)
		}
		return
	}
	notification := entry.Ch
	fullName := entry.Sym.FullName
	sess.notifications.lock.Unlock()

	var notificationTime time.Time
	if timestamp == 0 {
		notificationTime = time.Now()
	} else {
		timeStamp := int64(timestamp)/windowsTick - secToUnixEpoch
		notificationTime = time.Unix(timeStamp, int64(timestamp)%(windowsTick)*100)
	}
	// cache.lock for parse() — symbol fields live in cache.symbols and parse
	// mutates Value/Valid. Lock ordering: cache after notifications release
	// (never both held).
	// Re-resolve via cache.symbols[FullName]: the symbol fetched from
	// activeNotifications may be stranded post-reload (loadSymbols swapped
	// the cache between subscribe and now), in which case parse with the
	// FRESH cache.datatypes against the OLD symbol's DataType key may
	// mismatch. If the symbol is gone from the live cache, log + skip.
	sess.cache.lock.Lock()
	live := sess.cache.symbols[symbolKey(fullName)]
	if live == nil {
		sess.cache.lock.Unlock()
		sess.logger.Warn("notification target symbol no longer in cache; skipping parse",
			"handle", handle, "symbol", fullName)
		return
	}
	// R-CACHE-009 supplementary detection: 0-byte terminal sample =
	// symbol gone post-online-change. TwinCAT drops the old handle
	// silently after the symbol is deleted and emits one final 0-byte
	// sample on the now-dead handle. Intercept BEFORE the parse path so
	// the configured strategy fires (Ignore: log + callback; Close:
	// terminate; AutoReload: re-discover) and no spurious Update is
	// delivered for the dead handle.
	if len(content) == 0 && live.Length > 0 {
		dataType := live.DataType
		length := live.Length
		sess.cache.lock.Unlock()
		sess.logger.Debug("notification terminal 0-byte sample (symbol removed post-online-change)",
			"handle", handle, "symbol", fullName, "dataType", dataType, "expectedLength", length)
		sess.handleStaleDetection(ReturnCodeDeviceSymbolNoFound)
		return
	}
	value, err := live.parse(content, 0, sess.cache.datatypes)
	if err != nil {
		sess.cache.lock.Unlock()
		sess.logger.Error("error during parse of notification",
			"handle", handle, "symbol", fullName, "dataType", live.DataType, "error", err)
		return
	}
	sess.cache.lock.Unlock()

	sess.logger.Log(context.Background(), LevelTrace, "update received", "update", value)
	updateStruct := &Update{
		Variable:  fullName,
		Value:     value,
		TimeStamp: notificationTime,
	}
	// One-shot Stale flag (R-NOT-017): consume on first delivered sample.
	if reason, ok := sess.consumeStaleFlag(handle); ok {
		updateStruct.Stale = &StaleInfo{Reason: reason}
	}
	sess.deliverNotification(ctx, notification, updateStruct, handle, fullName)
}

// deliverNotification performs a non-blocking send on the caller-owned channel.
// Guards against the caller closing the channel: a select with default does NOT
// prevent panics on send-to-closed-channel — Go runtime always panics in that case.
// Recovers and logs an Error instead of crashing the listen goroutine.
//
// Caller must NOT close the update channel while subscriptions exist on this
// connection; see AddSymbolNotification(s) godoc for the ownership rule.
func (sess *Session) deliverNotification(ctx context.Context, ch chan<- *Update, update *Update, handle uint32, fullName string) {
	defer func() {
		if r := recover(); r != nil {
			sess.logger.Error("notification send panicked — caller closed the update channel?",
				"handle", handle,
				"symbol", fullName,
				"panic", r)
		}
	}()
	// Non-blocking send: deliver notification instantly or drop if channel full.
	// Caller controls backpressure by sizing the channel buffer appropriately
	// (e.g. make(chan *Update, 1024) for burst absorption).
	// This prevents goroutine accumulation and never blocks the receive pipeline.
	select {
	case <-ctx.Done():
	case ch <- update:
		sess.logger.Debug("Successfully delivered notification", "handle", handle)
	default:
		sess.logger.Warn("notification dropped (channel full, receiver too slow)",
			"handle", handle,
			"symbol", fullName)
	}
}

// Subscribe race window: the PLC-side notification handle exists from the
// moment AddDeviceNotification returns, but our activeNotifications insert
// happens afterwards — and on TC2, which answers 0x0701 to the sum command,
// AddSymbolNotifications degrades to one Add per symbol, so the last symbol
// of a 40-entry batch commits hundreds of milliseconds after the first
// symbol started streaming. Samples arriving in that window must be neither
// dropped nor mistaken for leaked handles.
const (
	// subscribeRaceWindow extends the guard past the last commit, covering
	// the gap between the insert and the in-flight counter reaching zero.
	subscribeRaceWindow = 100 * time.Millisecond
	// earlySampleMaxHandles bounds the buffer. Reaching it means far more
	// unknown handles are in flight than any real subscribe produces.
	earlySampleMaxHandles = 4096
	// subscribeRaceMaxOpen caps how long an in-flight subscribe may keep the
	// reaper suppressed, so a wedged one cannot disable it indefinitely.
	subscribeRaceMaxOpen = 30 * time.Second
	// earlySampleMaxBytes bounds the buffer in memory as well as in entries.
	// Generous for its purpose — one sample per symbol being subscribed — while
	// keeping a flood of large unknown-handle samples from growing without limit.
	earlySampleMaxBytes = 8 << 20
)

// subscribeRaceActive reports whether an unknown handle should be presumed to
// be one of ours mid-registration rather than a leaked one.
//
// The in-flight counter is authoritative but not unconditional: a subscribe that
// wedges would otherwise disable the orphan reaper for the life of the session,
// and the reaper exists to stop the PLC's handle table filling up. So the window
// also expires. subscribeRaceMaxOpen is generous enough for any real batch —
// hundreds of symbols registered one at a time on a slow PLC — while still
// bounded.
func (sess *Session) subscribeRaceActive() bool {
	mgr := sess.notifications
	now := time.Now().UnixNano()
	// Judged on the MOST RECENTLY opened subscribe still in flight. That is the
	// one whose handles the PLC may be streaming right now, so while it is inside
	// the cap the window stays open — even if an older sibling has wedged. And a
	// wedged subscribe on its own cannot hold the window open forever, because
	// nothing younger is there to vouch for it.
	//
	// Neither half works with a single timestamp slot: written on the 0 -> 1
	// transition it records when the last quiet period ended rather than when
	// anything in flight began, so sustained overlap pinned it to a call that had
	// already returned and the window closed under a subscribe that was still
	// registering. Measuring from the oldest open subscribe has the same failure
	// for the same reason. Only per-subscribe starts answer both questions.
	if newest, open := mgr.newestOpenSubscribe(); open {
		return now-newest < subscribeRaceMaxOpen.Nanoseconds()
	}
	// Nothing in flight: the short tail covers the gap between the last commit
	// and the PLC's first sample for it.
	return now-mgr.lastSubscribeNs.Load() < subscribeRaceWindow.Nanoseconds()
}

// newestOpenSubscribe returns the start time of the most recently opened
// subscribe that has not closed yet. open is false when none are in flight.
func (m *notificationManager) newestOpenSubscribe() (int64, bool) {
	m.openMu.Lock()
	defer m.openMu.Unlock()
	newest := int64(0)
	for _, start := range m.openSubscribes {
		if start > newest {
			newest = start
		}
	}
	return newest, newest != 0
}

// beginSubscribe marks a subscribe operation as in flight and returns the token
// that closes it. Every call must be paired with endSubscribe, which is why
// callers defer it immediately.
//
// The token exists so each open subscribe is tracked individually. Sharing one
// counter and one timestamp made the pair non-atomic across goroutines — an
// ending subscribe could clear the clock a starting one had just written, and the
// resulting "in flight but no clock" state suppressed the orphan reaper
// permanently.
func (sess *Session) beginSubscribe() subscribeToken {
	mgr := sess.notifications
	now := time.Now().UnixNano()

	mgr.openMu.Lock()
	mgr.nextSubscribeToken++
	tok := subscribeToken(mgr.nextSubscribeToken)
	if mgr.openSubscribes == nil {
		mgr.openSubscribes = make(map[subscribeToken]int64)
	}
	mgr.openSubscribes[tok] = now
	mgr.subscribeInFlight.Store(int64(len(mgr.openSubscribes)))
	mgr.openMu.Unlock()

	mgr.lastSubscribeNs.Store(now)
	return tok
}

// endSubscribe closes a subscribe operation: it replays samples buffered for
// the handles that were committed, then — once no subscribe is left in flight
// — discards what remains. A leftover entry belongs to a handle whose commit
// never happened (rejected item, stranded cache, TOCTOU loss); if it really is
// leaked PLC-side it keeps firing, and its next sample takes the orphan path
// normally.
//
// MUST be called after notifications.lock is released: the replay path takes
// cache.lock, and holding both is forbidden. Deferring it before the
// lock.Unlock defer gives that ordering for free (defers run LIFO).
func (sess *Session) endSubscribe(ctx context.Context, tok subscribeToken, committed []uint32) {
	mgr := sess.notifications
	mgr.lastSubscribeNs.Store(time.Now().UnixNano())
	sess.replayEarlySamples(ctx, committed)

	// Closing this subscribe and deciding whether anything may be discarded is one
	// critical section. Split apart, a subscribe beginning between them would see
	// the window still open, buffer a sample, and have this call throw it away.
	// Both locks are leaves and are always taken in this order, openMu first.
	mgr.openMu.Lock()
	delete(mgr.openSubscribes, tok)
	remaining := len(mgr.openSubscribes)
	mgr.subscribeInFlight.Store(int64(remaining))

	mgr.earlyMu.Lock()
	dropped := 0
	if remaining == 0 {
		dropped = len(mgr.earlySamples)
		if dropped > 0 {
			mgr.earlySamples = nil
			mgr.earlyBytes = 0
		}
	}
	mgr.earlyMu.Unlock()
	mgr.openMu.Unlock()

	if dropped > 0 {
		sess.logger.Debug("discarded buffered samples for handles that never committed", "count", dropped)
	}
}

// bufferEarlySample parks the most recent sample for an uncommitted handle.
func (sess *Session) bufferEarlySample(ctx context.Context, handle uint32, timestamp uint64, content []byte) {
	mgr := sess.notifications
	// Copy: content is freshly allocated per sample today, but the handler
	// signature is exported and this buffer outlives the dispatch call.
	buf := make([]byte, len(content))
	copy(buf, content)

	mgr.earlyMu.Lock()
	if mgr.earlySamples == nil {
		mgr.earlySamples = make(map[uint32]earlySample)
	}
	prev, known := mgr.earlySamples[handle]
	// Replacing an entry frees its bytes, so only the delta counts.
	delta := len(buf)
	if known {
		delta -= len(prev.content)
	}
	full := (!known && len(mgr.earlySamples) >= earlySampleMaxHandles) ||
		mgr.earlyBytes+delta > earlySampleMaxBytes
	if !full {
		mgr.earlySamples[handle] = earlySample{timestamp: timestamp, content: buf}
		mgr.earlyBytes += delta
	}
	held := mgr.earlyBytes
	mgr.earlyMu.Unlock()

	if full {
		sess.logger.Warn("early notification sample dropped (buffer full)",
			"handle", handle, "max_handles", earlySampleMaxHandles,
			"max_bytes", earlySampleMaxBytes, "held_bytes", held)
		return
	}
	sess.logger.Debug("buffered early notification sample (handle registration still in flight)", "handle", handle)

	// The commit can land between the map miss that sent us here and the insert
	// above. If it did, the replay that would have collected this sample has
	// already run, and no later one will look for a handle that is already
	// bound — so the sample would sit parked until some unrelated subscribe
	// discarded it. For a static symbol that is the only sample it will ever
	// send. Whoever loses that race cleans up after itself: replayEarlySamples
	// takes the sample only if the handle is bound by now, so this is a no-op
	// in the common case.
	sess.replayEarlySamples(ctx, []uint32{handle})
}

// replayEarlySamples dispatches buffered samples for handles that are bound in
// activeNotifications, removing them from the buffer as it goes.
//
// A handle that is NOT bound is left parked on purpose: its commit may still be
// coming, and dispatching it would take the unknown-handle path and schedule an
// orphan delete for a handle we may be about to own.
func (sess *Session) replayEarlySamples(ctx context.Context, handles []uint32) {
	if len(handles) == 0 {
		return
	}
	mgr := sess.notifications
	type replay struct {
		handle uint32
		sample earlySample
	}
	var pending []replay

	// notifications.lock first and released before earlyMu — the two are never
	// held together, in either order, anywhere.
	bound := make(map[uint32]struct{}, len(handles))
	mgr.lock.Lock()
	for _, h := range handles {
		if _, ok := mgr.activeNotifications[h]; ok {
			bound[h] = struct{}{}
		}
	}
	mgr.lock.Unlock()

	mgr.earlyMu.Lock()
	for _, h := range handles {
		if _, ok := bound[h]; !ok {
			continue
		}
		if s, ok := mgr.earlySamples[h]; ok {
			delete(mgr.earlySamples, h)
			mgr.earlyBytes -= len(s.content)
			pending = append(pending, replay{handle: h, sample: s})
		}
	}
	mgr.earlyMu.Unlock()

	for _, p := range pending {
		sess.logger.Debug("replaying buffered early notification sample", "handle", p.handle)
		sess.dispatchSample(ctx, p.handle, p.sample.timestamp, p.sample.content, false)
	}
}

// Orphan-Delete: when handleNotification receives a sample for a handle that
// is not in activeNotifications (and we are past the first-sample-race
// window), the handle is most likely a leftover subscription from a prior
// process that shared our source NetID+port. The PLC's notification handle
// table is finite per source identity; without explicit cleanup repeated
// process restarts accumulate orphan entries until the TwinCAT AMS router
// runs out of slots and starts rejecting new Adds or crashes outright
// (Beckhoff issue #268).
//
// Mitigation: issue an asynchronous DeleteDeviceNotification for the orphan
// handle. Constraints:
//   - Throttle per-handle so a high-rate orphan stream (PLC re-firing every
//     PLC cycle on a still-live subscription) doesn't spam Delete RPCs.
//   - Bound concurrency so a burst of N orphans on session resume doesn't
//     spawn N goroutines simultaneously.
//   - Re-check that the handle is STILL absent from activeNotifications
//     immediately before sending the RPC: between scheduling and firing, a
//     concurrent AddSymbolNotification can have committed this very handle.
//     Not because IDs get recycled — measured on TC2 and TC3, allocation is
//     monotonic (+1 per Add, no reuse after a Delete, no restart from a low
//     number for a fresh source NetID) — but because the sample that looked
//     orphaned may simply have arrived before its own commit landed. Deleting
//     our own just-acquired subscription would produce a permanent
//     orphan-loop. The re-check eliminates that race.
//   - Track the goroutine via lifecycle.waitGroup so Close waits for any
//     in-flight Delete to complete instead of leaving zombie goroutines.
//   - panic recover defensively — an unexpected panic here must not kill
//     the listen goroutine that called handleNotification.
const (
	orphanDeleteThrottle       = 60 * time.Second
	orphanDeleteMaxConcurrency = 10
	orphanDeleteSeenMaxAge     = 5 * time.Minute
	orphanDeleteRPCTimeout     = 5 * time.Second
)

// isBestEffortDeleteSuccess reports whether a DeleteDeviceNotification
// return code counts as cleanup success for best-effort paths.
// NoErrors                = actually deleted.
// NotifyHandleInvalid (0x714) = handle already gone on PLC side
//
//	(route-idle-timeout, PLC reboot, prior cleanup).
//
// DeviceClientUnknown  (0x715) = PLC dropped our client identity (typical
//
//	after TCP reset / reconnect); whatever
//	handles we had are implicitly gone too.
//
// In all three cases the handle is no longer consuming PLC resources,
// which is the only goal of best-effort cleanup paths.
//
// Note: Beckhoff's official AdsLib does NOT treat 0x715 as cleanup-success.
// This library does, because go-ads's reconnect path frequently hits
// 0x715 when PLC drops the client identity tied to the just-severed TCP,
// and treating it as failure produces misleading WARN spam during normal
// recovery. Net effect on cleanup correctness is identical: in both cases
// the PLC handle is gone.
func isBestEffortDeleteSuccess(code ReturnCode) bool {
	return code == ReturnCodeNoErrors ||
		code == ReturnCodeDeviceNotifyHandleInvalid ||
		code == ReturnCodeDeviceClientUnknown
}

// isBestEffortDeleteSuccessErr is the error-wrapped variant of
// isBestEffortDeleteSuccess for call sites that receive a Go error rather
// than a bare ReturnCode (e.g., orphan-Delete's RPC return). Unwraps the
// error chain via errors.Is for ReturnCodeDeviceNotifyHandleInvalid and
// ReturnCodeDeviceClientUnknown — the two codes that mean "PLC has
// nothing of yours to free".
func isBestEffortDeleteSuccessErr(err error) bool {
	if err == nil {
		return true
	}
	return errors.Is(err, ReturnCodeDeviceNotifyHandleInvalid) ||
		errors.Is(err, ReturnCodeDeviceClientUnknown)
}

// tryOrphanDelete schedules an async best-effort Delete of an unknown
// notification handle. Skip paths log Debug. RPC outcome:
//   - success         → Info (operators see productive cleanup)
//   - 0x714 NotifyHandleInvalid → Debug (PLC already reaped, expected)
//   - 0x715 DeviceClientUnknown → Debug (PLC dropped client identity,
//     handle implicitly gone)
//   - any other error → Warn (real failure: transport, auth, timeout,
//     marshaling, protocol mismatch)
//
// orphanDeleteAbortReason re-checks, immediately before the RPC, whether this
// handle might be ours after all. Both windows it covers open between scheduling
// the delete and firing it, and deleting our own live subscription is the
// failure this whole area exists to prevent — so the checks are a named function
// that can be tested directly rather than inline in a goroutine that a test can
// only race against.
func (sess *Session) orphanDeleteAbortReason(handle uint32) (string, bool) {
	mgr := sess.notifications
	// A concurrent subscribe may have committed this very handle since the delete
	// was scheduled: handle IDs are not recycled (allocation is monotonic on both
	// TC2 and TC3), but a sample can arrive before its own commit lands.
	mgr.lock.Lock()
	_, present := mgr.activeNotifications[handle]
	mgr.lock.Unlock()
	if present {
		return "handle reappeared in activeNotifications", true
	}
	// Or a subscribe started after we scheduled this and simply has not
	// committed its handles yet, so absence is not proof of a leak.
	if mgr.subscribeInFlight.Load() > 0 {
		return "a subscribe is in flight", true
	}
	return "", false
}

func (sess *Session) tryOrphanDelete(handle uint32) {
	// Lifecycle guards: never fire during shutdown or active reconnect.
	if sess.isClosed() || sess.isReconnecting() {
		sess.logger.Debug("orphan delete skipped: session closing or reconnecting", "handle", handle)
		return
	}

	// Throttle map + bounded sem may be uninitialised on Session{} struct
	// literals used in some tests. Skip silently — these tests don't
	// exercise the orphan path.
	mgr := sess.notifications
	if mgr.orphanSeen == nil || mgr.orphanSem == nil {
		return
	}

	// Throttle check + GC old entries under orphanMu. Don't write the
	// throttle entry yet — only commit it after we've successfully
	// acquired a sem slot, otherwise a sem-full drop would 60s-lock the
	// handle even though no RPC fired.
	mgr.orphanMu.Lock()
	now := time.Now()
	cutoff := now.Add(-orphanDeleteSeenMaxAge)
	for h, t := range mgr.orphanSeen {
		if t.Before(cutoff) {
			delete(mgr.orphanSeen, h)
		}
	}
	if last, seen := mgr.orphanSeen[handle]; seen && now.Sub(last) < orphanDeleteThrottle {
		mgr.orphanMu.Unlock()
		sess.logger.Debug("orphan delete throttled (recent attempt for same handle)",
			"handle", handle,
			"last_attempt_ago", now.Sub(last))
		return
	}
	mgr.orphanMu.Unlock()

	// Bounded concurrency: non-blocking acquire. If sem full, drop this
	// attempt without writing throttle entry; the next orphan sample
	// retries immediately rather than waiting out a 60s throttle window.
	select {
	case mgr.orphanSem <- struct{}{}:
	default:
		sess.logger.Debug("orphan delete skipped: max concurrent deletes in flight",
			"handle", handle,
			"max_concurrent", orphanDeleteMaxConcurrency)
		return
	}

	// Record the attempt now, before scheduling: a high-rate orphan stream would
	// otherwise slip several deletes through the gap before the goroutine runs.
	// The goroutine clears it again if it aborts, so an attempt that sends no RPC
	// does not lock the handle out for the throttle window.
	mgr.orphanMu.Lock()
	mgr.orphanSeen[handle] = now
	mgr.orphanMu.Unlock()

	// Track the goroutine so Close waits for in-flight orphan deletes. Via
	// trackGoroutine, because the isClosed() check far above is a TOCTOU: Close can
	// finish in the gap and its Wait return before this Add lands, which panics the
	// process. Refusal means releasing what was reserved for the goroutine.
	started := sess.trackGoroutine(func() {
		defer func() { <-mgr.orphanSem }()
		defer func() {
			if r := recover(); r != nil {
				sess.logger.Error("orphan delete goroutine panic recovered",
					"handle", handle, "panic", r)
			}
		}()

		if reason, abort := sess.orphanDeleteAbortReason(handle); abort {
			// No RPC went out, so do not hold the throttle: a genuinely leaked
			// handle would otherwise go unreaped for the whole window because it
			// happened to fire while someone was subscribing.
			mgr.orphanMu.Lock()
			delete(mgr.orphanSeen, handle)
			mgr.orphanMu.Unlock()
			sess.logger.Debug("orphan delete aborted", "handle", handle, "reason", reason)
			return
		}

		c := sess.client.Load()
		if c == nil {
			sess.logger.Debug("orphan delete aborted: client not initialised", "handle", handle)
			return
		}

		// Snapshot lifecycle.ctx under ctxMu.RLock — Reconnect replaces it
		// via tearDownAndReset under ctxMu.Lock, so a raw read races with
		// the swap. RLock + snapshot matches the pattern at session.go's
		// Close/triggerReconnect call sites.
		sess.lifecycle.ctxMu.RLock()
		parentCtx := sess.lifecycle.ctx
		sess.lifecycle.ctxMu.RUnlock()
		if err := parentCtx.Err(); err != nil {
			sess.logger.Debug("orphan delete aborted: lifecycle context done", "handle", handle, "error", err)
			return
		}

		ctx, cancel := context.WithTimeout(parentCtx, orphanDeleteRPCTimeout)
		defer cancel()
		if err := c.DeleteDeviceNotification(ctx, handle); err != nil {
			// 0x714 NotifyHandleInvalid = expected (PLC already reaped via
			// route-idle-timeout, reboot, or prior cleanup pass).
			// 0x715 DeviceClientUnknown = PLC dropped our client identity
			// entirely (typical after TCP reset / reconnect); the handle
			// went with it. Both Debug so they don't flood under high-rate
			// orphan streams.
			// Every other code (transport, auth, timeout, marshaling,
			// protocol mismatch) is a real failure operators need to see;
			// surface at Warn.
			if isBestEffortDeleteSuccessErr(err) {
				sess.logger.Debug("orphan delete RPC: handle already gone PLC-side (expected)",
					"handle", handle, "error", err)
				return
			}
			sess.logger.Warn("orphan delete RPC failed",
				"handle", handle, "error", err)
			return
		}
		// State the observation, not a guess at the cause. The old wording
		// claimed the handle "was leaked by a prior session/process" — during
		// the v2.2.0 subscribe-race regression it was saying that about
		// subscriptions this very session had created milliseconds earlier,
		// which sent the diagnosis in the wrong direction for months.
		sess.logger.Info("deleted a PLC notification handle this session does not own",
			"handle", handle,
			"hint", "usually a subscription left behind by an earlier process sharing this source NetID and port")
	})
	if !started {
		// Release what was reserved for a goroutine that will not run: the semaphore
		// slot, and the throttle entry — a genuinely leaked handle should not be
		// locked out for the whole window because it fired as the session closed.
		<-mgr.orphanSem
		mgr.orphanMu.Lock()
		delete(mgr.orphanSeen, handle)
		mgr.orphanMu.Unlock()
		sess.logger.Debug("orphan delete not started: the session is closed", "handle", handle)
	}
}

// Heartbeat: proving the caller's subscriptions are still alive without asking the
// PLC anything.
//
// See notificationManager.heartbeatHandle for the measurements this rests on. In
// short: a runtime restart kills subscriptions while leaving the connection, the
// symbol version and the ADS state unchanged, so there is no inbound event to react
// to — but one CYCLIC subscription of our own turns silence into proof, because
// TwinCAT pushes those on a timer whether the value changes or not.
const (
	defaultHeartbeatInterval = 2 * time.Second
	defaultHeartbeatMissed   = 5
	// maxADSCycleTime is the longest cycle an ADS notification can carry: the
	// wire field is 32-bit 100ns ticks.
	maxADSCycleTime = 400 * time.Second

	// maxHeartbeatRecoveryBackoff caps the wait between recovery attempts once they
	// start failing. A PLC left in CONFIG is the normal reason, and it can sit there
	// for hours; the session still has to come back on its own within a sensible
	// time of the runtime serving again.
	maxHeartbeatRecoveryBackoff = 30 * time.Second

	// maxFailureBackoffShift bounds the doubling at base * 64, so the shift itself
	// can never run off into a nonsense window if failures keep counting up.
	maxFailureBackoffShift = 6
)

// heartbeatAllowedTicks reports how many silent ticks the watcher tolerates before
// it attempts a recovery: the base window, doubled once per consecutive failed
// recovery, bounded by maxHeartbeatRecoveryBackoff in wall-clock terms.
//
// Pure and separate from the watcher because the two ways this arithmetic goes
// wrong are both invisible from the outside — the session simply retries at the
// wrong rate — and a previous version of this fix was lost precisely because
// nothing pinned it.
//
// The cap is a duration converted to ticks, which is why it is floored at base:
//   - At a long cycle the budget is FEWER ticks than the base window, so applying
//     it raw makes the first failure shrink the tolerated silence instead of
//     growing it (base 5 at a 10s cycle: 50s -> 30s) — backoff running backwards.
//   - Once cycle >= the budget the division truncates to 0. Skipping the cap on
//     that (the old `capped > 0` guard) let the window grow to base<<6 unbounded:
//     at a 31s cycle the effective wait was hours.
//
// The floor answers both: it keeps the cap from ever reducing the window, and it
// keeps the cap binding when the division has nothing left to say.
func heartbeatAllowedTicks(base, consecutiveFailures int, cycle time.Duration) int {
	if consecutiveFailures <= 0 || base <= 0 || cycle <= 0 {
		return base
	}
	shift := min(consecutiveFailures, maxFailureBackoffShift)
	capTicks := max(int(maxHeartbeatRecoveryBackoff/cycle), base)
	return min(base<<shift, capTicks)
}

// heartbeatEnabled reports whether this session keeps a heartbeat.
func (sess *Session) heartbeatEnabled() bool {
	return !sess.heartbeatDisabled
}

func (sess *Session) heartbeatCycle() time.Duration {
	if sess.heartbeatInterval > 0 {
		return sess.heartbeatInterval
	}
	return defaultHeartbeatInterval
}

func (sess *Session) heartbeatAllowedMisses() int {
	if sess.heartbeatMissed >= 2 {
		return sess.heartbeatMissed
	}
	return defaultHeartbeatMissed
}

// establishHeartbeat registers the internal cyclic notification, if enabled and not
// already present. Failing is not fatal: the session works, it just loses the
// ability to notice its subscriptions dying quietly.
func (sess *Session) establishHeartbeat(ctx context.Context) {
	// Same window as recoverDeadSubscriptions: registering a beat on a session that
	// has already released its PLC resources strands it.
	if sess.isClosed() || !sess.heartbeatEnabled() || sess.notifications.heartbeatHandle.Load() != 0 {
		return
	}
	c := sess.client.Load()
	if c == nil {
		return
	}
	// Cyclic, one byte, on the symbol-version group: runtime-served (so it dies
	// with the runtime's notification table, which is the event being detected),
	// present regardless of the caller's program, and its payload is the version.
	handle, err := c.AddDeviceNotification(ctx, uint32(GroupSymbolVersion), 0, 1,
		TransModeServerCycle, 0, sess.heartbeatCycle())
	if err != nil {
		// First failure is worth a Warn; the rest are Debug, because the watcher
		// retries on a cadence and a device that refuses cyclic notifications
		// altogether would otherwise produce one Warn per interval forever.
		msg := "could not establish the notification heartbeat; retrying, and until it succeeds a silent subscription death will go unnoticed"
		if sess.notifications.heartbeatEstablishFailures.Add(1) == 1 {
			sess.logger.Warn(msg, "error", err)
		} else {
			sess.logger.Debug(msg, "error", err)
		}
		// Start the watcher anyway. It used to run only after a SUCCESSFUL establish,
		// so a session whose first attempt failed had no watchdog for its entire life
		// — no retry, and a later silent death went unnoticed with one Warn as the
		// only trace. The watcher re-attempts the beat itself (see heartbeatWatch).
		sess.startHeartbeatWatch()
		return
	}
	sess.notifications.heartbeatEstablishFailures.Store(0)
	// A handle that is already one of the caller's would make every sample for
	// that subscription look like a beat and be swallowed. No real PLC issues
	// duplicates, but the consequence is bad enough to check for.
	sess.notifications.lock.Lock()
	_, collides := sess.notifications.activeNotifications[handle]
	sess.notifications.lock.Unlock()
	if collides {
		sess.logger.Warn("PLC returned a heartbeat handle that is already in use by a subscription; not using it",
			"handle", handle)
		if derr := c.DeleteDeviceNotification(ctx, handle); derr != nil {
			sess.logger.Debug("releasing the colliding heartbeat handle failed", "error", derr)
		}
		return
	}
	// CompareAndSwap, not Store: two concurrent first subscribes both see no
	// heartbeat, both register one, and the second Store would orphan the first —
	// a cyclic registration the PLC keeps pushing that belongs to nothing. The
	// loser deletes what it just created.
	if !sess.notifications.heartbeatHandle.CompareAndSwap(0, handle) {
		sess.logger.Debug("another subscribe established the heartbeat first; releasing this one", "handle", handle)
		if derr := c.DeleteDeviceNotification(ctx, handle); derr != nil {
			sess.logger.Debug("releasing the redundant heartbeat handle failed", "handle", handle, "error", derr)
		}
		return
	}
	sess.notifications.heartbeatLastNs.Store(time.Now().UnixNano())
	sess.logger.Debug("notification heartbeat established", "handle", handle, "cycle", sess.heartbeatCycle())
	sess.startHeartbeatWatch()
}

// consumeHeartbeat records a beat. Returns true when the sample was the heartbeat
// and must not reach the caller.
func (sess *Session) consumeHeartbeat(handle uint32, content []byte) bool {
	if handle == 0 || handle != sess.notifications.heartbeatHandle.Load() {
		return false
	}
	sess.notifications.heartbeatBeats.Add(1)
	sess.notifications.heartbeatLastNs.Store(time.Now().UnixNano())
	// The payload is the symbol version, so the beat carries online-change
	// detection for free — no extra request needed.
	if len(content) > 0 {
		// Record what we saw before acting on it, under the same lock that read the
		// old value. Without the write-back every following beat re-detects the same
		// change: under SymbolVersionIgnore that is a markAllHandlesStale and a fresh
		// versionCallback goroutine per interval, forever, against the documented
		// once-per-detection contract. AutoReload only escaped it because LoadSymbols
		// rewrites the field on its way through.
		sess.cache.lock.Lock()
		known := sess.cache.symbolVersion
		changed := known != 0 && content[0] != known
		if changed {
			sess.cache.symbolVersion = content[0]
		}
		sess.cache.lock.Unlock()
		if changed {
			sess.logger.Info("symbol version changed (seen on the heartbeat)", "old", known, "new", content[0])
			sess.handleStaleDetection(ReturnCodeDeviceSymbolVersionInvalid)
		}
	}
	return true
}

// startHeartbeatWatch runs the watcher once per session.
func (sess *Session) startHeartbeatWatch() {
	sess.heartbeatOnce.Do(func() {
		// Through the admission gate, not a bare Add: Close now waits this group, so
		// a raw Add is the same TOCTOU spawnMu exists to close. establishHeartbeat
		// runs on a USER goroutine and does a full PLC round-trip between its
		// isClosed() check and this call, which is plenty of room for Close to
		// complete its own Wait — and an Add landing after that is documented
		// WaitGroup misuse, i.e. a process-level panic.
		if !sess.trackGoroutineOn(&sess.heartbeatWG, sess.heartbeatWatch) {
			sess.logger.Debug("not starting the heartbeat watch: the session is closed")
		}
	})
}

// heartbeatWatch re-subscribes when the beats stop.
//
// Retrying matters as much as detecting: while the PLC is in CONFIG the
// re-subscribe cannot succeed, and that is fine — no data is expected then. The
// watcher keeps trying so the session recovers by itself the moment the runtime is
// serving again, which is the whole requirement.
//
// But it retries on a leash. Measured in our own integration run against
// 192.168.3.224: 1468 copies of the silence warning, 28% of the entire log, each
// followed by "batch add notification failed: ads: client transport closed". Two
// causes, both fixed here — a closed transport was treated exactly like a PLC in
// CONFIG (retry on the very next tick), and failures did not slow anything down.
// Each attempt also re-queues every config, so the resubscribe attempt counters
// were being burned at the heartbeat interval.
func (sess *Session) heartbeatWatch() {
	// No Done here: trackGoroutineOn owns the Add and the Done.
	cycle := sess.heartbeatCycle()
	// lastBeats/quietTicks are the whole state of the detector: how many beats had
	// arrived at the previous tick, and how many ticks have passed with none.
	var lastBeats uint64
	quietTicks := 0
	ticker := time.NewTicker(cycle)
	defer ticker.Stop()

	// consecutiveFailures backs the retry off and keeps the log to one line per
	// episode. Goroutine-local: this is the only writer.
	consecutiveFailures := 0

	for {
		select {
		case <-sess.lifecycle.closedCh:
			return
		case <-ticker.C:
		}
		// Connected, not merely "not disconnected": dialAndStart clears
		// tx.disconnected before the route, reload and resubscribe steps run, so a
		// reconnect's tail looks live here. Ticking through it accumulates quiet
		// ticks against subscriptions the reconnect is in the middle of restoring,
		// and can trigger a full delete-and-re-add of handles it has just
		// registered — correct, thanks to resubscribeMu, but pure churn.
		if sess.lifecycle.state.load() != SessionStateConnected {
			continue // a drop has its own recovery path; do not compete with it
		}
		// A transport that is gone cannot carry a re-subscribe, and getting it back
		// is the reconnect path's job, not ours. Without this the watcher spun at
		// the heartbeat interval for the life of the process on any session whose
		// client died while the FSM still said Connected.
		if c := sess.client.Load(); c == nil || (c.ctx != nil && c.ctx.Err() != nil) {
			continue
		}
		sess.notifications.lock.Lock()
		active := len(sess.notifications.activeNotifications)
		wanted := len(sess.notifications.pending)
		sess.notifications.lock.Unlock()
		if wanted == 0 && active == 0 {
			continue // the caller has asked for nothing; nothing to protect
		}
		// No special case for "wanted but none active": a failed recovery leaves the
		// heartbeat clock stale, so the silence check below fires again on the next
		// tick and retries. Verified by removing this path and watching the test
		// still pass, which is the definition of code not worth keeping.
		// Silence measured in ticks of this ticker, not in wall-clock time: the
		// ticker is monotonic, so a clock step cannot make a healthy session look
		// dead (or a dead one look healthy). See notificationManager.heartbeatBeats.
		beats := sess.notifications.heartbeatBeats.Load()
		if beats != lastBeats {
			lastBeats = beats
			quietTicks = 0
			continue
		}
		quietTicks++
		// Each consecutive failed recovery doubles the tolerated silence, capped, so
		// a PLC that stays in CONFIG for an hour costs a handful of attempts instead
		// of one per interval.
		allowed := heartbeatAllowedTicks(sess.heartbeatAllowedMisses(), consecutiveFailures, cycle)
		if quietTicks < allowed {
			continue
		}
		// No beat registered at all: re-attempt that first. It is the cheap
		// explanation for silence — one Add, versus tearing down and re-subscribing
		// everything — and it is the only way a session whose first attempt failed
		// ever gets a watchdog. Only if the beat is in place does continued silence
		// mean the subscriptions are dead.
		if sess.notifications.heartbeatHandle.Load() == 0 {
			quietTicks = 0
			// Counted as a failure so the interval grows: a device that refuses
			// cyclic notifications outright would otherwise get a round-trip every
			// `allowed` ticks forever. The log is throttled already; the request
			// rate was not.
			consecutiveFailures++
			ctx, cancel := context.WithTimeout(sess.currentLifecycleCtx(), cycle)
			sess.establishHeartbeat(ctx)
			cancel()
			continue
		}
		quietTicks = 0

		// One Warn per episode. Repeats go to Debug: the operator needs to know the
		// subscriptions died, not to be told again every interval until they recover.
		msg := "no notification heartbeat within the allowed window; treating this session's subscriptions as dead and re-subscribing"
		args := []any{
			"cycle", cycle, "missedTicks", allowed,
			// Wall clock, so only ever informational — the decision above is made in
			// ticks. A stepped clock makes this number odd, not the outcome wrong.
			"silentForApprox", time.Duration(allowed) * cycle,
			"detail", "a runtime restart or CONFIG toggle stops delivery without dropping the connection, " +
				"changing the symbol version or reporting an error",
		}
		if consecutiveFailures == 0 {
			sess.logger.Warn(msg, args...)
		} else {
			sess.logger.Debug(msg, append(args, "retry", consecutiveFailures)...)
		}
		switch sess.recoverDeadSubscriptions() {
		case recoveryDone:
			consecutiveFailures = 0
		case recoveryDeferred:
			// Nothing was attempted, so this says nothing about how hard recovery is.
			// Counting it doubled the wait every window while a runtime sat in CONFIG,
			// so by the time it returned to RUN the next attempt was minutes out —
			// measured on 192.168.3.118, which never recovered inside a 2 minute grace.
		default:
			consecutiveFailures++
		}
	}
}

// recoverDeadSubscriptions releases what the PLC may still hold and re-subscribes
// from the stored configs, then re-establishes the heartbeat.
//
// Reports the outcome so the watcher can tell the three cases apart: recovered,
// failed (back off), and deferred because the runtime is not serving (do not back
// off — nothing was attempted, and the state poll will say when to try again).
type recoveryOutcome int

const (
	recoveryFailed recoveryOutcome = iota
	recoveryDone
	recoveryDeferred
)

func (sess *Session) recoverDeadSubscriptions() recoveryOutcome {
	// heartbeatWatch checks this too, but that check is a TOCTOU: Close can land
	// immediately after it. Close marks the session closed and releases the PLC
	// resources BEFORE cancelling the context, so a recovery entering that window
	// still has a live transport and its registrations land after the release that
	// was meant to be the last word — handles nobody will ever delete, streaming
	// into a channel the caller considers finished.
	// Atomic with markClosed, not a bare check: Close marks the session closed and
	// releases its PLC resources before cancelling the context, and a concurrent
	// Reconnect re-derives lifecycle.ctx from the (uncancelled) parent — so a
	// recovery that passed a bare check could register handles over a freshly
	// dialled transport AFTER the release meant to be terminal.
	if !sess.admitBackgroundWork() {
		sess.logger.Debug("skipping subscription recovery: the session is closed")
		return recoveryFailed
	}
	// Before touching anything: a runtime that is not serving cannot accept a
	// re-subscribe, so releasing the handles and attempting one only to fail is pure
	// churn — and the attempt used to be counted as a failure, which doubled the
	// backoff every window. On hardware that meant the session was minutes away from
	// its next try by the time the runtime came back.
	if state, known := sess.knownRuntimeState(); known && runtimeDefinitelyNotServing(state) {
		sess.logger.Info("re-subscribe deferred: the PLC runtime is not in RUN",
			"state", uint16(state))
		return recoveryDeferred
	}

	ctx := sess.currentLifecycleCtx()

	// Held across the whole sequence below — snapshot the intent, try, restore on
	// failure — so no other re-subscribe can run between the snapshot and the
	// restore. Serialising only the attempt would still let a reconnect's
	// re-subscribe read the intent this one has already cleared.
	sess.notifications.resubscribeMu.Lock()
	defer sess.notifications.resubscribeMu.Unlock()

	// The transport is alive here — only the PLC's notification table died — so
	// quiesce dispatch. Takes the old heartbeat with it, so a stale handle cannot
	// be mistaken for a beat once a new one is registered, and so
	// establishHeartbeat below is not a no-op.
	//
	// bestEffortDeleteNotifications runs with userTeardown=false, so
	// notificationChannel survives for the resubscribe that follows.
	stale := sess.takeNotificationHandles(true)
	sess.releaseNotificationHandles(ctx, stale, "dead subscriptions before re-subscribing")

	// Snapshot the caller's intent before trying. resubscribeNotifications treats
	// a failed attempt as one of a bounded number of retries and drops the config
	// after three — which is right for a reconnect, and wrong here: while the PLC
	// is in CONFIG every attempt fails, and burning the budget threw away
	// subscriptions the caller never cancelled. Measured on hardware: three
	// refusals and the session was silent for good.
	sess.notifications.lock.Lock()
	intent := make([]pendingNotification, len(sess.notifications.pending))
	copy(intent, sess.notifications.pending)
	channel := sess.notifications.notificationChannel
	sess.notifications.lock.Unlock()

	restoreIntent := func() {
		sess.notifications.lock.Lock()
		// Merge, not overwrite: a subscribe that landed while this attempt was in
		// flight must not be erased by a snapshot taken before it existed.
		sess.notifications.restoreConfigs(intent)
		if sess.notifications.notificationChannel == nil {
			sess.notifications.notificationChannel = channel
		}
		sess.notifications.lock.Unlock()
	}

	if err := sess.resubscribeNotificationsLocked(); err != nil {
		restoreIntent()
		if errors.Is(err, ErrRuntimeNotRunning) {
			// The gate refused mid-flight: the runtime stopped serving between the
			// check above and the attempt. Deferred, not failed.
			sess.logger.Info("re-subscribe deferred: the PLC runtime stopped serving", "error", err)
			return recoveryDeferred
		}
		sess.logger.Warn("re-subscribe after a heartbeat timeout failed; keeping the subscriptions on file and retrying in the next window",
			"error", err, "configs", len(intent))
		return recoveryFailed
	}
	// A "successful" resubscribe that bound nothing is the CONFIG case: the PLC
	// refused every item. Same treatment — keep the intent, try again later.
	sess.notifications.lock.Lock()
	bound := len(sess.notifications.activeNotifications)
	sess.notifications.lock.Unlock()
	if bound == 0 && len(intent) > 0 {
		restoreIntent()
		sess.logger.Warn("re-subscribe bound nothing (PLC not serving yet); keeping the subscriptions on file and retrying in the next window",
			"configs", len(intent))
		return recoveryFailed
	}
	sess.notifications.heartbeatLastNs.Store(time.Now().UnixNano())
	sess.establishHeartbeat(ctx)
	sess.logger.Info("subscriptions re-established after the heartbeat stopped")
	return recoveryDone
}
