package ads

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/rs/zerolog/log"
)

const windowsTick int64 = 10000000
const secToUnixEpoch int64 = 11644473600

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
		log.Error().
			Int("handle", int(handle)).
			Msg("Can't find notification handle")
		return nil
	}
	datatypes := conn.datatypes
	notification := symbol.Notification

	timeStamp := int64(timestamp)/windowsTick - secToUnixEpoch
	notificationTime := time.Unix(timeStamp, int64(timestamp)%(windowsTick)*100)
	// parse() mutates Symbol fields (Value, Changed, Valid, etc.) so it must
	// run under lock. This is safe because parse is pure CPU work (no I/O).
	value, err := symbol.parse(content, 0, datatypes)
	if err != nil {
		conn.symbolLock.Unlock()
		log.Error().
			Err(err).
			Msg("error during parse of notification")
		return nil
	}
	symbol.Value = value
	conn.symbolLock.Unlock()

	log.Trace().
		Str("update", value).
		Msg("update received")
	updateStruct := &Update{
		Variable:  symbol.FullName,
		Value:     value,
		TimeStamp: notificationTime,
	}
	// Try to send the response to the waiting request function
	select {
	case <-ctx.Done():
	case notification <- updateStruct:
		log.Debug().
			Uint32("handle", handle).
			Msg("Successfully delivered notification")
	}
	return nil
}
