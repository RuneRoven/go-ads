package ads

import (
	"encoding/binary"
	"fmt"
	"time"
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
	if conn.sumNotifState.Load() == 2 {
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
		if isSumCommandUnsupportedError(err) {
			conn.sumNotifState.CompareAndSwap(0, 2) // atomic: only first prober sets
			conn.logger.Warn("SumAddDeviceNotification not supported, using individual calls", "error", err)
			return conn.sumAddNotificationFallback(requests)
		}
		return nil, nil, fmt.Errorf("SumAddDeviceNotification failed: %w", err)
	}

	conn.sumNotifState.CompareAndSwap(0, 1) // atomic: only first prober sets

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
	if conn.sumNotifState.Load() == 2 {
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
		if isSumCommandUnsupportedError(err) {
			conn.sumNotifState.CompareAndSwap(0, 2) // atomic: only first prober sets
			conn.logger.Warn("SumDeleteDeviceNotification not supported, using individual calls", "error", err)
			return conn.sumDeleteNotificationFallback(handles)
		}
		return nil, fmt.Errorf("SumDeleteDeviceNotification failed: %w", err)
	}

	conn.sumNotifState.CompareAndSwap(0, 1) // atomic: only first prober sets

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
			if sym := conn.activeNotifications[h]; sym != nil {
				conn.removeNotificationConfig(sym.FullName)
			}
			delete(conn.activeNotifications, h)
			conn.logger.Info("batch deleted notification handle", "handle", h)
		}
	}
	if len(conn.activeNotifications) == 0 {
		conn.notificationChannel = nil
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
			conn.logger.Info("downgraded transmission mode for older PLC",
				"from", req.TransmissionMode.String(),
				"to", transMode.String(),
				"index", i)
		}
		h, err := conn.AddDeviceNotification(req.Group, req.Offset, req.Length, transMode, req.MaxDelay, req.CycleTime)
		if err != nil {
			errors[i] = ReturnCodeDeviceError
			conn.logger.Warn("individual AddDeviceNotification failed in fallback", "error", err, "index", i)
		} else {
			errors[i] = ReturnCodeNoErrors
			handles[i] = h
		}
	}
	return handles, errors, nil
}

// bestEffortDeleteNotifications attempts to delete the given handles via
// SumDeleteDeviceNotification. Errors are logged but never returned — this is
// for cleanup paths where the caller cannot meaningfully react to a failure
// (e.g. PLC unreachable during a reconnect retry). Returns the count of
// successfully deleted handles for logging. Treats ReturnCodeDeviceNotifyHandleInvalid
// as success-equivalent (handle already gone PLC-side).
func (conn *Connection) bestEffortDeleteNotifications(handles []uint32) int {
	if len(handles) == 0 {
		return 0
	}
	errors, err := conn.SumDeleteDeviceNotification(handles)
	if err != nil {
		conn.logger.Warn("bestEffortDelete: SumDeleteDeviceNotification failed",
			"error", err,
			"handles", len(handles))
		return 0
	}
	deleted := 0
	for _, e := range errors {
		if e == ReturnCodeNoErrors || e == ReturnCodeDeviceNotifyHandleInvalid {
			deleted++
		}
	}
	if deleted < len(handles) {
		conn.logger.Warn("bestEffortDelete: some handles not cleaned up",
			"deleted", deleted,
			"requested", len(handles))
	}
	return deleted
}

// sumDeleteNotificationFallback deletes notifications individually when sum commands are not supported.
func (conn *Connection) sumDeleteNotificationFallback(handles []uint32) ([]ReturnCode, error) {
	errors := make([]ReturnCode, len(handles))
	for i, h := range handles {
		err := conn.DeleteDeviceNotification(h)
		if err != nil {
			errors[i] = ReturnCodeDeviceError
			conn.logger.Warn("individual DeleteDeviceNotification failed in fallback", "error", err, "handle", h)
		} else {
			errors[i] = ReturnCodeNoErrors
		}
	}
	return errors, nil
}
