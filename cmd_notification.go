package ads

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"time"
)

// Single-symbol device-notification raw RPCs on *Client:
// AddDeviceNotification, DeleteDeviceNotification. Notifs persistence
// + activeNotifications cleanup is the Session's wrapper concern (see
// Session.DeleteDeviceNotification below). The cache-aware
// handleNotification dispatcher (installed via Client.SetNotificationHandler)
// also lives in this file because it shares Update / handle bookkeeping.

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
	resp, err := c.sendRequest(CommandIDAddDeviceNotification, request.Bytes())
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
func (c *Client) DeleteDeviceNotification(handle uint32) error {
	request := &bytes.Buffer{}
	type deleteNotificationCommandPacket struct {
		Handle uint32
	}
	content := deleteNotificationCommandPacket{handle}
	if err := binary.Write(request, binary.LittleEndian, content); err != nil {
		return fmt.Errorf("binary.Write failed: %w", err)
	}
	resp, err := c.sendRequest(CommandIDDeleteDeviceNotification, request.Bytes())
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
func (sess *Session) DeleteDeviceNotification(handle uint32) error {
	if err := sess.client.Load().DeleteDeviceNotification(handle); err != nil {
		return err
	}
	sess.notifications.lock.Lock()
	if sym := sess.notifications.activeNotifications[handle]; sym != nil {
		sess.removeNotificationConfig(sym.FullName)
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
// PLC. Successfully deleted handles (or handle-invalid, treated as
// success-equivalent) are removed from activeNotifications.
func (sess *Session) SumDeleteDeviceNotification(handles []uint32) ([]ReturnCode, error) {
	errors, err := sess.client.Load().SumDeleteDeviceNotification(handles)
	if err != nil {
		return nil, err
	}
	if len(errors) == 0 {
		return errors, nil
	}
	sess.notifications.lock.Lock()
	for i, h := range handles {
		if errors[i] == ReturnCodeNoErrors || errors[i] == ReturnCodeDeviceNotifyHandleInvalid {
			if sym := sess.notifications.activeNotifications[h]; sym != nil {
				sess.removeNotificationConfig(sym.FullName)
			}
			delete(sess.notifications.activeNotifications, h)
			sess.logger.Info("batch deleted notification handle", "handle", h, "errorCode", uint32(errors[i]))
		}
	}
	if len(sess.notifications.activeNotifications) == 0 {
		sess.notifications.notificationChannel = nil
	}
	sess.notifications.lock.Unlock()
	return errors, nil
}

const (
	windowsTick    int64 = 10000000
	secToUnixEpoch int64 = 11644473600
)

type NotificationStream struct {
	Length uint32
	Stamps uint32
}
type StampHeader struct {
	Timestamp uint64
	Samples   uint32
}
type NotificationSample struct {
	Handle uint32
	Size   uint32
}

// DeviceNotification (ADS cmd 8) packet decoder lives on *Client — see
// Client.deviceNotification. Session.handleNotification below is the
// cache-aware handler installed via Client.SetNotificationHandler from
// Session.Connect.

func (sess *Session) handleNotification(ctx context.Context, handle uint32, timestamp uint64, content []byte) {
	// notifications.lock: handle lookup + symbol pointer/field snapshot.
	sess.notifications.lock.Lock()
	symbol, ok := sess.notifications.activeNotifications[handle]
	if !ok {
		sess.notifications.lock.Unlock()
		// Stale notifications are expected during:
		// - Close(): handles deleted from activeNotifications while listen() still drains
		// - Reconnect: activeNotifications cleared (connection.go:575) before new subscriptions
		// - first-sample race — PLC fires the first notification before our
		//   activeNotifications map insert completes (sub-millisecond window for
		//   fast PLCs and zero-MaxDelay subscriptions). Suppress within ~100ms of
		//   the most recent successful subscribe.
		const subscribeRaceWindowNs = int64(100 * time.Millisecond)
		switch {
		case sess.isClosed() || sess.isReconnecting():
			sess.logger.Debug("received notification for deleted handle (expected during close/reconnect)", "handle", handle)
		case time.Now().UnixNano()-sess.notifications.lastSubscribeNs.Load() < subscribeRaceWindowNs:
			sess.logger.Debug("received notification for unknown handle (likely first-sample race)", "handle", handle)
		default:
			sess.logger.Warn("received notification for unknown handle", "handle", handle)
		}
		return
	}
	notification := symbol.Notification
	fullName := symbol.FullName
	sess.notifications.lock.Unlock()

	var notificationTime time.Time
	if timestamp == 0 {
		notificationTime = time.Now()
	} else {
		timeStamp := int64(timestamp)/windowsTick - secToUnixEpoch
		notificationTime = time.Unix(timeStamp, int64(timestamp)%(windowsTick)*100)
	}
	// cache.lock for parse() — Symbol fields live in cache.symbols and parse
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
		updateStruct.Stale = true
		updateStruct.Reason = reason
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
