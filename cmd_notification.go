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
		c.logger.Error("failed to parse notification response", "error", err)
		return 0, err
	}
	if notificationResponse.Error != 0 {
		c.logger.Error("failed to add notification handler", "errorCode", uint32(notificationResponse.Error))
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
	c.logger.Info("deleted handle", "handle", handle)
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
	if entry, ok := sess.notifications.activeNotifications[handle]; ok && entry.Sym != nil {
		symbolName = entry.Sym.FullName
	}
	sess.notifications.lock.Unlock()

	if err := sess.client.Load().DeleteDeviceNotification(ctx, handle); err != nil {
		return err
	}
	sess.notifications.lock.Lock()
	if symbolName != "" {
		sess.removeNotificationConfig(symbolName)
	}
	delete(sess.notifications.activeNotifications, handle)
	if len(sess.notifications.activeNotifications) == 0 {
		sess.notifications.notificationChannel = nil
	}
	sess.notifications.lock.Unlock()
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
	for i := 0; i < limit; i++ {
		if !isBestEffortDeleteSuccess(codes[i]) {
			continue
		}
		h := handles[i]
		if entry, ok := sess.notifications.activeNotifications[h]; ok && entry.Sym != nil {
			sess.removeNotificationConfig(entry.Sym.FullName)
		}
		delete(sess.notifications.activeNotifications, h)
		sess.logger.Info("batch deleted notification handle", "handle", h, "errorCode", uint32(codes[i]))
	}
	if len(sess.notifications.activeNotifications) == 0 {
		sess.notifications.notificationChannel = nil
	}
	sess.notifications.lock.Unlock()
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
			sess.bufferEarlySample(handle, timestamp, content)
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
)

// subscribeRaceActive reports whether an unknown handle should be presumed to
// be one of ours mid-registration rather than a leaked one.
func (sess *Session) subscribeRaceActive() bool {
	mgr := sess.notifications
	if mgr.subscribeInFlight.Load() > 0 {
		return true
	}
	return time.Now().UnixNano()-mgr.lastSubscribeNs.Load() < subscribeRaceWindow.Nanoseconds()
}

// beginSubscribe marks a subscribe operation as in flight. Every call must be
// paired with endSubscribe, which is why callers defer it immediately.
func (sess *Session) beginSubscribe() {
	sess.notifications.subscribeInFlight.Add(1)
	sess.notifications.lastSubscribeNs.Store(time.Now().UnixNano())
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
func (sess *Session) endSubscribe(ctx context.Context, committed []uint32) {
	mgr := sess.notifications
	mgr.lastSubscribeNs.Store(time.Now().UnixNano())
	sess.replayEarlySamples(ctx, committed)
	inFlight := mgr.subscribeInFlight.Add(-1)
	if inFlight > 0 {
		return
	}
	mgr.earlyMu.Lock()
	dropped := len(mgr.earlySamples)
	if dropped > 0 {
		mgr.earlySamples = nil
	}
	mgr.earlyMu.Unlock()
	if dropped > 0 {
		sess.logger.Debug("discarded buffered samples for handles that never committed", "count", dropped)
	}
}

// bufferEarlySample parks the most recent sample for an uncommitted handle.
func (sess *Session) bufferEarlySample(handle uint32, timestamp uint64, content []byte) {
	mgr := sess.notifications
	// Copy: content is freshly allocated per sample today, but the handler
	// signature is exported and this buffer outlives the dispatch call.
	buf := make([]byte, len(content))
	copy(buf, content)

	mgr.earlyMu.Lock()
	if mgr.earlySamples == nil {
		mgr.earlySamples = make(map[uint32]earlySample)
	}
	_, known := mgr.earlySamples[handle]
	full := !known && len(mgr.earlySamples) >= earlySampleMaxHandles
	if !full {
		mgr.earlySamples[handle] = earlySample{timestamp: timestamp, content: buf}
	}
	mgr.earlyMu.Unlock()

	if full {
		sess.logger.Warn("early notification sample dropped (buffer full)",
			"handle", handle, "max_handles", earlySampleMaxHandles)
		return
	}
	sess.logger.Debug("buffered early notification sample (handle registration still in flight)", "handle", handle)
}

// replayEarlySamples re-dispatches buffered samples for handles that have since
// been committed to activeNotifications.
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

	mgr.earlyMu.Lock()
	for _, h := range handles {
		if s, ok := mgr.earlySamples[h]; ok {
			delete(mgr.earlySamples, h)
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
//     immediately before sending the RPC: between scheduling and firing,
//     a concurrent AddSymbolNotification could legitimately have received
//     the same handle ID back from the PLC (PLC reuses IDs from a freed
//     table slot). Deleting our own just-acquired subscription would
//     produce a permanent orphan-loop. The re-check eliminates that race.
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

	// Commit throttle entry now that the slot is reserved.
	mgr.orphanMu.Lock()
	mgr.orphanSeen[handle] = now
	mgr.orphanMu.Unlock()

	// Track goroutine so Close waits for in-flight orphan deletes.
	sess.lifecycle.waitGroup.Add(1)
	go func() {
		defer sess.lifecycle.waitGroup.Done()
		defer func() { <-mgr.orphanSem }()
		defer func() {
			if r := recover(); r != nil {
				sess.logger.Error("orphan delete goroutine panic recovered",
					"handle", handle, "panic", r)
			}
		}()

		// Race guard: between scheduling and firing, a concurrent
		// AddSymbolNotification may have legitimately received this
		// handle from the PLC. If so, DO NOT delete it — we'd kill our
		// own just-established subscription. Re-check under
		// notifications.lock to close the window.
		mgr.lock.Lock()
		_, present := mgr.activeNotifications[handle]
		mgr.lock.Unlock()
		if present {
			sess.logger.Debug("orphan delete aborted: handle reappeared in activeNotifications between scheduling and firing",
				"handle", handle)
			return
		}
		// Second guard for the same window: a subscribe that started after
		// this Delete was scheduled may hold this handle and simply not have
		// committed it yet, so absence from activeNotifications is not proof
		// the handle is leaked.
		if mgr.subscribeInFlight.Load() > 0 {
			sess.logger.Debug("orphan delete aborted: a subscribe is in flight", "handle", handle)
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
		sess.logger.Info("orphan PLC notification handle deleted (was leaked by prior session/process)",
			"handle", handle)
	}()
}
