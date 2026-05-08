package ads

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"time"
)

// Single-symbol device-notification commands: AddDeviceNotification,
// DeleteDeviceNotification. The dispatcher for incoming notification packets
// (handleNotification + deliverNotification) lives here too because it shares
// the Update / handle bookkeeping defined by these commands.

func (conn *Session) AddDeviceNotification(
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

	content := addDeviceNotificationCommandPacket{
		group,
		offset,
		length,
		uint32(transmissionMode),
		uint32(maxDelay.Nanoseconds() / 100),  // ADS uses 100ns units (1ms = 10000)
		uint32(cycleTime.Nanoseconds() / 100), // ADS uses 100ns units (1ms = 10000)
		[16]byte{},
	}
	if err = binary.Write(request, binary.LittleEndian, content); err != nil {
		return 0, fmt.Errorf("binary.Write failed: %w", err)
	}
	type addDeviceNotificationResponse struct {
		Error  ReturnCode
		Handle uint32
	}
	// Try to send the request
	resp, err := conn.sendRequest(CommandIDAddDeviceNotification, request.Bytes())
	if err != nil {
		return
	}
	respBuffer := bytes.NewBuffer(resp)
	notificationResponse := addDeviceNotificationResponse{}
	err = binary.Read(respBuffer, binary.LittleEndian, &notificationResponse)
	if err != nil {
		conn.logger.Error("failed to parse notification response", "error", err)
		return 0, err
	}
	if notificationResponse.Error != 0 {
		conn.logger.Error("failed to add notification handler", "errorCode", uint32(notificationResponse.Error))
		return 0, fmt.Errorf("unable to create notification: %w", notificationResponse.Error)
	}
	conn.logger.Log(context.Background(), LevelTrace, "added notification handler", "handle", notificationResponse.Handle)

	return notificationResponse.Handle, nil
}

// DeleteDeviceNotification deletes a device notification by handle.
func (conn *Session) DeleteDeviceNotification(handle uint32) error {
	request := &bytes.Buffer{}
	type deleteNotificationCommandPacket struct {
		Handle uint32
	}
	content := deleteNotificationCommandPacket{
		handle,
	}
	if err := binary.Write(request, binary.LittleEndian, content); err != nil {
		return fmt.Errorf("binary.Write failed: %w", err)
	}
	// Try to send the request
	resp, err := conn.sendRequest(CommandIDDeleteDeviceNotification, request.Bytes())
	if err != nil {
		conn.logger.Warn("error deleting handle", "handle", handle, "error", err)
		return err
	}

	// Check the result error code
	respBuffer := bytes.NewBuffer(resp)
	var adsError ReturnCode
	if err = binary.Read(respBuffer, binary.LittleEndian, &adsError); err != nil {
		return fmt.Errorf("failed to parse DeleteDeviceNotification response: %w", err)
	}
	if adsError > 0 {
		conn.logger.Warn("error deleting handle", "handle", handle, "errorCode", uint32(adsError))
		return fmt.Errorf("ADS error in DeleteDeviceNotification: %w", adsError)
	}
	conn.notifs.lock.Lock()
	if sym := conn.notifs.activeNotifications[handle]; sym != nil {
		conn.removeNotificationConfig(sym.FullName)
	}
	delete(conn.notifs.activeNotifications, handle)
	if len(conn.notifs.activeNotifications) == 0 {
		conn.notifs.notificationChannel = nil
	}
	conn.notifs.lock.Unlock()
	conn.logger.Info("deleted handle", "handle", handle)
	return nil
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

// DeviceNotification - ADS command id: 8
func (conn *Session) DeviceNotification(ctx context.Context, in []byte) error {
	var stream NotificationStream
	var header StampHeader
	var sample NotificationSample
	var content []byte

	data := bytes.NewBuffer(in)

	// Read stream header

	err := binary.Read(data, binary.LittleEndian, &stream)
	if err != nil {
		return fmt.Errorf("unable to read notification: %w", err)
	}
	for i := uint32(0); i < stream.Stamps; i++ {
		// Read stamp header
		if err = binary.Read(data, binary.LittleEndian, &header); err != nil {
			return fmt.Errorf("error reading stamp header: %w", err)
		}

		for j := uint32(0); j < header.Samples; j++ {
			if err = binary.Read(data, binary.LittleEndian, &sample); err != nil {
				return fmt.Errorf("error reading notification sample: %w", err)
			}
			if sample.Size > uint32(data.Len()) {
				return fmt.Errorf("notification sample size %d exceeds remaining data %d", sample.Size, data.Len())
			}
			content = make([]byte, sample.Size)
			n, err := data.Read(content)
			if err != nil {
				return fmt.Errorf("error reading notification content: %w", err)
			}
			if n != int(sample.Size) {
				return fmt.Errorf("short read on notification content: got %d of %d bytes", n, sample.Size)
			}
			conn.handleNotification(ctx, sample.Handle, header.Timestamp, content)
		}
	}
	return nil
}

func (conn *Session) handleNotification(ctx context.Context, handle uint32, timestamp uint64, content []byte) {
	// Phase 1: notifs.lock for handle lookup + symbol pointer/field snapshot.
	conn.notifs.lock.Lock()
	symbol, ok := conn.notifs.activeNotifications[handle]
	if !ok {
		conn.notifs.lock.Unlock()
		// Stale notifications are expected during:
		// - Close(): handles deleted from activeNotifications while listen() still drains
		// - Reconnect: activeNotifications cleared (connection.go:575) before new subscriptions
		// - first-sample race — PLC fires the first notification before our
		//   activeNotifications map insert completes (sub-millisecond window for
		//   fast PLCs and zero-MaxDelay subscriptions). Suppress within ~100ms of
		//   the most recent successful subscribe.
		const subscribeRaceWindowNs = int64(100 * time.Millisecond)
		switch {
		case conn.isClosed() || conn.lifecycle.reconnecting.Load():
			conn.logger.Debug("received notification for deleted handle (expected during close/reconnect)", "handle", handle)
		case time.Now().UnixNano()-conn.notifs.lastSubscribeNs.Load() < subscribeRaceWindowNs:
			conn.logger.Debug("received notification for unknown handle (likely first-sample race)", "handle", handle)
		default:
			conn.logger.Warn("received notification for unknown handle", "handle", handle)
		}
		return
	}
	notification := symbol.Notification
	fullName := symbol.FullName
	conn.notifs.lock.Unlock()

	var notificationTime time.Time
	if timestamp == 0 {
		notificationTime = time.Now()
	} else {
		timeStamp := int64(timestamp)/windowsTick - secToUnixEpoch
		notificationTime = time.Unix(timeStamp, int64(timestamp)%(windowsTick)*100)
	}
	// Phase 2: cache.lock for parse() — Symbol fields live in cache.symbols
	// and parse mutates Value/Valid. Lock ordering: cache after notifs
	// release (never both held).
	// Re-resolve via cache.symbols[FullName]: the symbol fetched from
	// activeNotifications may be stranded post-reload (loadSymbols swapped
	// the cache between subscribe and now), in which case parse with the
	// FRESH cache.datatypes against the OLD symbol's DataType key may
	// mismatch. If the symbol is gone from the live cache, log + skip.
	conn.cache.lock.Lock()
	live := conn.cache.symbols[symbolKey(fullName)]
	if live == nil {
		conn.cache.lock.Unlock()
		conn.logger.Warn("notification target symbol no longer in cache; skipping parse",
			"handle", handle, "symbol", fullName)
		return
	}
	value, err := live.parse(content, 0, conn.cache.datatypes)
	if err != nil {
		conn.cache.lock.Unlock()
		conn.logger.Error("error during parse of notification",
			"handle", handle, "symbol", fullName, "dataType", live.DataType, "error", err)
		return
	}
	conn.cache.lock.Unlock()

	conn.logger.Log(context.Background(), LevelTrace, "update received", "update", value)
	updateStruct := &Update{
		Variable:  fullName,
		Value:     value,
		TimeStamp: notificationTime,
	}
	conn.deliverNotification(ctx, notification, updateStruct, handle, fullName)
}

// deliverNotification performs a non-blocking send on the caller-owned channel.
// Guards against the caller closing the channel: a select with default does NOT
// prevent panics on send-to-closed-channel — Go runtime always panics in that case.
// Recovers and logs an Error instead of crashing the listen goroutine.
//
// Caller must NOT close the update channel while subscriptions exist on this
// connection; see AddSymbolNotification(s) godoc for the ownership rule.
func (conn *Session) deliverNotification(ctx context.Context, ch chan<- *Update, update *Update, handle uint32, fullName string) {
	defer func() {
		if r := recover(); r != nil {
			conn.logger.Error("notification send panicked — caller closed the update channel?",
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
		conn.logger.Debug("Successfully delivered notification", "handle", handle)
	default:
		conn.logger.Warn("notification dropped (channel full, receiver too slow)",
			"handle", handle,
			"symbol", fullName)
	}
}
