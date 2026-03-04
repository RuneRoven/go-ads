package ads

import (
	"encoding/binary"
	"fmt"
	"time"

	"github.com/rs/zerolog/log"
)

// SumNotificationRequest represents a single notification add request within a batch.
type SumNotificationRequest struct {
	Group            uint32
	Offset           uint32
	Length           uint32
	TransmissionMode TransMode
	MaxDelay         time.Duration
	CycleTime        time.Duration
}

// SumAddDeviceNotification adds multiple device notifications in a single ADS round-trip
// using GroupSumupAddDeviceNotification (0xF085).
// Falls back to individual AddDeviceNotification calls on older PLCs.
func (conn *Connection) SumAddDeviceNotification(requests []SumNotificationRequest) (handles []uint32, errors []ReturnCode, err error) {
	if len(requests) == 0 {
		return nil, nil, nil
	}

	// Skip sum command if we already know it's not supported
	if conn.sumReadChecked.Load() && !conn.sumReadSupported.Load() {
		return conn.sumAddNotificationFallback(requests)
	}

	n := len(requests)

	// Each request: Group(4) + Offset(4) + Length(4) + TransMode(4) + MaxDelay(4) + CycleTime(4) + Reserved(16) = 40 bytes
	writeData := make([]byte, n*40)
	for i, req := range requests {
		off := i * 40
		binary.LittleEndian.PutUint32(writeData[off:], req.Group)
		binary.LittleEndian.PutUint32(writeData[off+4:], req.Offset)
		binary.LittleEndian.PutUint32(writeData[off+8:], req.Length)
		binary.LittleEndian.PutUint32(writeData[off+12:], uint32(req.TransmissionMode))
		binary.LittleEndian.PutUint32(writeData[off+16:], uint32(req.MaxDelay.Nanoseconds()/100))
		binary.LittleEndian.PutUint32(writeData[off+20:], uint32(req.CycleTime.Nanoseconds()/100))
		// bytes 24-39 are reserved (zero)
	}

	// Response: N × (error(4) + handle(4)) = N × 8 bytes
	readLen := uint32(n * 8)

	resp, err := conn.WriteRead(uint32(GroupSumupAddDeviceNotification), uint32(n), readLen, writeData)
	if err != nil {
		if !conn.sumReadChecked.Load() {
			log.Warn().Err(err).Msg("Sum commands not supported by PLC, using individual calls")
			conn.sumReadSupported.Store(false)
			conn.sumReadChecked.Store(true)
		}
		return conn.sumAddNotificationFallback(requests)
	}

	if !conn.sumReadChecked.Load() {
		conn.sumReadSupported.Store(true)
		conn.sumReadChecked.Store(true)
	}

	if len(resp) < n*8 {
		return nil, nil, fmt.Errorf("SumAddDeviceNotification response too short: got %d bytes, expected %d", len(resp), n*8)
	}

	handles = make([]uint32, n)
	errors = make([]ReturnCode, n)
	for i := 0; i < n; i++ {
		errors[i] = ReturnCode(binary.LittleEndian.Uint32(resp[i*8:]))
		handles[i] = binary.LittleEndian.Uint32(resp[i*8+4:])
	}

	return handles, errors, nil
}

// SumDeleteDeviceNotification deletes multiple device notifications in a single ADS round-trip
// using GroupSumupDeleteDeviceNotification (0xF086).
// Falls back to individual DeleteDeviceNotification calls on older PLCs.
func (conn *Connection) SumDeleteDeviceNotification(handles []uint32) ([]ReturnCode, error) {
	if len(handles) == 0 {
		return nil, nil
	}

	// Skip sum command if we already know it's not supported
	if conn.sumReadChecked.Load() && !conn.sumReadSupported.Load() {
		return conn.sumDeleteNotificationFallback(handles)
	}

	n := len(handles)

	// Write data: N × handle(4)
	writeData := make([]byte, n*4)
	for i, h := range handles {
		binary.LittleEndian.PutUint32(writeData[i*4:], h)
	}

	// Response: N × error(4)
	readLen := uint32(n * 4)

	resp, err := conn.WriteRead(uint32(GroupSumupDeleteDeviceNotification), uint32(n), readLen, writeData)
	if err != nil {
		if !conn.sumReadChecked.Load() {
			log.Warn().Err(err).Msg("Sum commands not supported by PLC, using individual calls")
			conn.sumReadSupported.Store(false)
			conn.sumReadChecked.Store(true)
		}
		return conn.sumDeleteNotificationFallback(handles)
	}

	if !conn.sumReadChecked.Load() {
		conn.sumReadSupported.Store(true)
		conn.sumReadChecked.Store(true)
	}

	if len(resp) < n*4 {
		return nil, fmt.Errorf("SumDeleteDeviceNotification response too short: got %d bytes, expected %d", len(resp), n*4)
	}

	errors := make([]ReturnCode, n)
	for i := 0; i < n; i++ {
		errors[i] = ReturnCode(binary.LittleEndian.Uint32(resp[i*4:]))
	}

	// Clean up internal notification tracking
	conn.symbolLock.Lock()
	for i, h := range handles {
		if errors[i] == ReturnCodeNoErrors {
			delete(conn.activeNotifications, h)
			log.Info().Uint32("handle", h).Msg("batch deleted notification handle")
		}
	}
	conn.symbolLock.Unlock()

	return errors, nil
}

// sumAddNotificationFallback adds notifications individually when sum commands are not supported.
// It also downgrades v2 transmission modes to v1 equivalents since older PLCs silently ignore v2 modes.
func (conn *Connection) sumAddNotificationFallback(requests []SumNotificationRequest) ([]uint32, []ReturnCode, error) {
	handles := make([]uint32, len(requests))
	errors := make([]ReturnCode, len(requests))
	for i, req := range requests {
		// Downgrade v2 modes for older PLCs that don't support them
		transMode := downgradeTransMode(req.TransmissionMode)
		if transMode != req.TransmissionMode {
			log.Info().
				Str("from", req.TransmissionMode.String()).
				Str("to", transMode.String()).
				Int("index", i).
				Msg("downgraded transmission mode for older PLC")
		}
		h, err := conn.AddDeviceNotification(req.Group, req.Offset, req.Length, transMode, req.MaxDelay, req.CycleTime)
		if err != nil {
			errors[i] = ReturnCodeDeviceError
			log.Warn().Err(err).Int("index", i).Msg("individual AddDeviceNotification failed in fallback")
		} else {
			errors[i] = ReturnCodeNoErrors
			handles[i] = h
		}
	}
	return handles, errors, nil
}

// sumDeleteNotificationFallback deletes notifications individually when sum commands are not supported.
func (conn *Connection) sumDeleteNotificationFallback(handles []uint32) ([]ReturnCode, error) {
	errors := make([]ReturnCode, len(handles))
	for i, h := range handles {
		err := conn.DeleteDeviceNotification(h)
		if err != nil {
			errors[i] = ReturnCodeDeviceError
			log.Warn().Err(err).Uint32("handle", h).Msg("individual DeleteDeviceNotification failed in fallback")
		} else {
			errors[i] = ReturnCodeNoErrors
		}
	}
	return errors, nil
}
