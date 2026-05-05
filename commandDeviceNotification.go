package ads

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"time"
)

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
func (conn *Connection) DeviceNotification(ctx context.Context, in []byte) error {
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

func (conn *Connection) handleNotification(ctx context.Context, handle uint32, timestamp uint64, content []byte) error {
	// Read all needed data under the lock, then release before channel send
	// to avoid deadlock if the receiver calls back into Connection methods.
	conn.symbolLock.Lock()
	symbol, ok := conn.activeNotifications[handle]
	if !ok {
		conn.symbolLock.Unlock()
		// Stale notifications are expected during:
		// - Close(): handles deleted from activeNotifications while listen() still drains
		// - Reconnect: activeNotifications cleared (connection.go:575) before new subscriptions
		if conn.closed.Load() || conn.reconnecting.Load() {
			conn.logger.Debug("received notification for deleted handle (expected during close/reconnect)", "handle", handle)
		} else {
			conn.logger.Warn("received notification for unknown handle", "handle", handle)
		}
		return nil
	}
	datatypes := conn.datatypes
	notification := symbol.Notification
	fullName := symbol.FullName

	var notificationTime time.Time
	if timestamp == 0 {
		notificationTime = time.Now()
	} else {
		timeStamp := int64(timestamp)/windowsTick - secToUnixEpoch
		notificationTime = time.Unix(timeStamp, int64(timestamp)%(windowsTick)*100)
	}
	// parse() mutates Symbol fields (Value, Changed, Valid, etc.) so it must
	// run under lock. This is safe because parse is pure CPU work (no I/O).
	value, err := symbol.parse(content, 0, datatypes)
	if err != nil {
		conn.symbolLock.Unlock()
		conn.logger.Error("error during parse of notification", "error", err)
		return nil
	}
	conn.symbolLock.Unlock()

	conn.logger.Log(context.Background(), LevelTrace, "update received", "update", value)
	updateStruct := &Update{
		Variable:  fullName,
		Value:     value,
		TimeStamp: notificationTime,
	}
	conn.deliverNotification(ctx, notification, updateStruct, handle, fullName)
	return nil
}

// deliverNotification performs a non-blocking send on the caller-owned channel.
// Guards against the caller closing the channel: a select with default does NOT
// prevent panics on send-to-closed-channel — Go runtime always panics in that case.
// Recovers and logs an Error instead of crashing the listen goroutine.
//
// Caller must NOT close the update channel while subscriptions exist on this
// connection; see AddSymbolNotification(s) godoc for the ownership rule.
func (conn *Connection) deliverNotification(ctx context.Context, ch chan<- *Update, update *Update, handle uint32, fullName string) {
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
