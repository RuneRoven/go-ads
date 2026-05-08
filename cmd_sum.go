package ads

import (
	"encoding/binary"
	"fmt"
	"math"
	"time"
)

// Batched ADS sum commands: SumRead, SumWrite, SumAddDeviceNotification,
// SumDeleteDeviceNotification. Each falls back to individual commands when
// the PLC does not support the sum variant. Capability state is tracked via
// the capabilities type on Session.

// sumCmdSpec describes a single sum-command's protocol contract for use
// with executeSumCommand. SumAddDeviceNotification and SumDeleteDeviceNotification
// fit this shape (fixed-size per-item request, fixed-size per-item response).
// SumRead has a two-command-ID fallback; SumWrite has a variable-size data
// section. Both retain custom orchestration — see their respective doc
// comments for rationale.
type sumCmdSpec[Req any, Res any] struct {
	stateLoad             func() uint32
	stateCASToSupported   func() bool
	stateCASToUnsupported func() bool
	group                 Group
	itemWriteSize         int
	itemReadSize          int
	encode                func(buf []byte, req Req)
	decode                func(resp []byte, n int) ([]Res, error)
	fallback              func(requests []Req) ([]Res, error)
}

// executeSumCommand orchestrates a sum command end-to-end:
//  1. If state is "unsupported" (2), call fallback directly.
//  2. Encode requests into a contiguous buffer of n × itemWriteSize bytes.
//  3. Issue WriteRead.
//  4. On error: if isSumCommandUnsupportedError, CAS state to unsupported,
//     log Warn, call fallback. Otherwise propagate.
//  5. On success: CAS state to supported, length-check response, decode.
func executeSumCommand[Req any, Res any](conn *Session, spec sumCmdSpec[Req, Res], requests []Req) ([]Res, error) {
	if len(requests) == 0 {
		return nil, nil
	}
	if spec.stateLoad() == 2 {
		return spec.fallback(requests)
	}
	n := len(requests)
	writeData := make([]byte, n*spec.itemWriteSize)
	for i, req := range requests {
		spec.encode(writeData[i*spec.itemWriteSize:(i+1)*spec.itemWriteSize], req)
	}
	readLen := uint32(n * spec.itemReadSize)
	resp, err := conn.WriteRead(uint32(spec.group), uint32(n), readLen, writeData)
	if err != nil {
		if isSumCommandUnsupportedError(err) {
			spec.stateCASToUnsupported()
			conn.logger.Warn("sum command not supported by PLC, using fallback",
				"group", uint32(spec.group),
				"error", err)
			return spec.fallback(requests)
		}
		return nil, fmt.Errorf("sum command (group 0x%X) failed: %w", uint32(spec.group), err)
	}
	spec.stateCASToSupported()
	if len(resp) < n*spec.itemReadSize {
		return nil, fmt.Errorf("sum command (group 0x%X) response too short: got %d, expected at least %d", uint32(spec.group), len(resp), n*spec.itemReadSize)
	}
	return spec.decode(resp, n)
}

// SumReadRequest represents a single read request within a sum/batch read.
type SumReadRequest struct {
	Group  uint32
	Offset uint32
	Length uint32
}

// SumReadResult represents the result of a single read within a sum/batch read.
type SumReadResult struct {
	Error ReturnCode
	Data  []byte
}

// SumRead performs a batch read of multiple index group/offset/length combinations
// in a single ADS round-trip.
//
// Beckhoff recommends using the newest command versions, so this tries:
//  1. SumReadEx2 (0xF084) — preferred, TC3 only
//  2. SumReadEx  (0xF083) — works on TC2 + TC3
//  3. Individual reads     — final fallback
//
// Both 0xF084 and 0xF083 use the same response format: [N × (error(4), length(4))][data].
// The detected command level is cached for subsequent calls and reset on reconnect.
//
// SumRead is intentionally NOT migrated to executeSumCommand. The two-command-ID
// fallback (Ex2 → Ex → individual) doesn't fit the helper's single-spec contract
// cleanly — migrating would require either two helper invocations or a spec
// extension that couples the helper to one caller's quirk. Custom orchestration
// is clearer here.
func (conn *Session) SumRead(requests []SumReadRequest) ([]SumReadResult, error) {
	if len(requests) == 0 {
		return nil, nil
	}

	cmd := conn.capabilities.SumReadCmdLoad()
	if cmd == 1 {
		// Already determined: no sum read support
		return conn.sumReadFallback(requests)
	}

	n := len(requests)

	// Build write data: N × 12 bytes (group + offset + length per request)
	writeData := make([]byte, n*12)
	var totalReadLen uint64
	for i, req := range requests {
		binary.LittleEndian.PutUint32(writeData[i*12:], req.Group)
		binary.LittleEndian.PutUint32(writeData[i*12+4:], req.Offset)
		binary.LittleEndian.PutUint32(writeData[i*12+8:], req.Length)
		totalReadLen += uint64(req.Length)
	}

	// Response: [N × (error(4), length(4))][data]
	total := uint64(n)*8 + totalReadLen
	if total > math.MaxUint32 {
		return nil, fmt.Errorf("SumRead total response size %d exceeds uint32 max", total)
	}
	readLen := uint32(total)

	if cmd != 0 {
		// Already probed — use cached command
		return conn.sumReadExec(Group(cmd), uint32(n), readLen, writeData, requests)
	}

	// First call: probe starting with Ex2, then Ex
	return conn.sumReadProbe(uint32(n), readLen, writeData, requests)
}

// sumReadProbe tries SumReadEx2 then SumReadEx, caching the first that works.
func (conn *Session) sumReadProbe(count uint32, readLen uint32, writeData []byte, requests []SumReadRequest) ([]SumReadResult, error) {
	// Try SumReadEx2 (0xF084) first
	resp, err := conn.WriteRead(uint32(GroupSumupReadEx2), count, readLen, writeData)
	if err == nil {
		conn.capabilities.SumReadCmdStore(uint32(GroupSumupReadEx2))
		conn.logger.Info("SumRead using SumReadEx2 (0xF084)")
		return conn.parseSumReadResponse(resp, int(count), requests)
	}
	if !isSumCommandUnsupportedError(err) {
		return nil, fmt.Errorf("SumRead failed: %w", err)
	}
	conn.logger.Info("SumReadEx2 not supported, trying SumReadEx", "error", err)

	// Try SumReadEx (0xF083)
	resp, err = conn.WriteRead(uint32(GroupSumupReadEx), count, readLen, writeData)
	if err == nil {
		conn.capabilities.SumReadCmdStore(uint32(GroupSumupReadEx))
		conn.logger.Info("SumRead using SumReadEx (0xF083)")
		return conn.parseSumReadResponse(resp, int(count), requests)
	}
	if !isSumCommandUnsupportedError(err) {
		return nil, fmt.Errorf("SumRead failed: %w", err)
	}
	conn.logger.Warn("SumReadEx not supported, using individual reads", "error", err)

	// No sum read support
	conn.capabilities.SumReadCmdStore(1)
	return conn.sumReadFallback(requests)
}

// sumReadExec performs a sum read with a known-good command.
func (conn *Session) sumReadExec(group Group, count uint32, readLen uint32, writeData []byte, requests []SumReadRequest) ([]SumReadResult, error) {
	resp, err := conn.WriteRead(uint32(group), count, readLen, writeData)
	if err != nil {
		return nil, fmt.Errorf("SumRead failed: %w", err)
	}
	return conn.parseSumReadResponse(resp, int(count), requests)
}

// parseSumReadResponse parses the [N × (error, length)][data] response format
// shared by both SumReadEx (0xF083) and SumReadEx2 (0xF084).
//
// Note: The official Beckhoff PDF shows 0xF083 with a separate error array (like 0xF080),
// but empirical testing on both TwinCAT 2 and TwinCAT 3 confirms 0xF083 returns the
// interleaved format identical to 0xF084. See PROTOCOL.md for details.
func (conn *Session) parseSumReadResponse(resp []byte, n int, requests []SumReadRequest) ([]SumReadResult, error) {
	if len(resp) < n*8 {
		return nil, fmt.Errorf("SumRead response too short: got %d bytes, expected at least %d", len(resp), n*8)
	}

	results := make([]SumReadResult, n)
	lengths := make([]uint32, n)
	for i := 0; i < n; i++ {
		results[i].Error = ReturnCode(binary.LittleEndian.Uint32(resp[i*8:]))
		lengths[i] = binary.LittleEndian.Uint32(resp[i*8+4:])
	}

	dataOffset := n * 8
	for i := 0; i < n; i++ {
		// Errored items: skip data-section validation but STILL advance dataOffset
		// by the declared length so subsequent items align. Beckhoff is silent
		// on whether failed items emit data bytes; we treat lengths[i] as the
		// truth and only bail if it would overflow the response buffer.
		if results[i].Error != ReturnCodeNoErrors {
			remaining := uint32(len(resp) - dataOffset)
			if lengths[i] > remaining {
				conn.logger.Error("SumRead errored-item declared length exceeds remaining bytes — protocol drift",
					"item_index", i,
					"declared_length", lengths[i],
					"bytes_remaining", remaining,
					"items_lost", n-i)
				for j := i; j < n; j++ {
					results[j].Error = ReturnCodeDeviceInvalidSize
				}
				break
			}
			dataOffset += int(lengths[i])
			continue
		}
		// Reject per-item undersize: protocol contract says a successful read
		// returns exactly requests[i].Length bytes. Anything shorter shifts
		// subsequent offsets in the data section.
		if lengths[i] < requests[i].Length {
			conn.logger.Error("SumRead per-item undersize — protocol drift",
				"item_index", i,
				"declared_length", lengths[i],
				"requested_length", requests[i].Length)
			for j := i; j < n; j++ {
				results[j].Error = ReturnCodeDeviceInvalidSize
			}
			break
		}
		// reject per-item oversize. PLC declaring a bigger length than
		// requested would shift all subsequent items' offsets even if total
		// bytes fit in the response. Reject and break (cascade marks remaining).
		if lengths[i] > requests[i].Length {
			conn.logger.Error("SumRead per-item oversize — protocol drift or wire corruption",
				"item_index", i,
				"declared_length", lengths[i],
				"requested_length", requests[i].Length)
			for j := i; j < n; j++ {
				results[j].Error = ReturnCodeDeviceInvalidSize
			}
			break
		}
		// Compare in uint32 space first to avoid int wrap on 32-bit Go.
		// On 32-bit, int(uint32(0xFFFFFFFE)) = -2, so dataOffset + that wraps
		// below len(resp) and the guard misfires; allocations after that panic.
		remaining := uint32(len(resp) - dataOffset)
		if lengths[i] > remaining {
			// Data section is position-dependent: each item's offset depends on
			// the cumulative lengths of all preceding items. Once one item is
			// truncated, all subsequent offsets are wrong and unrecoverable.
			conn.logger.Error("SumRead truncated — likely protocol corruption",
				"items_lost", n-i,
				"declared_length", lengths[i],
				"bytes_remaining", remaining,
				"item_index", i)
			for j := i; j < n; j++ {
				results[j].Error = ReturnCodeDeviceInvalidSize
			}
			break
		}
		end := dataOffset + int(lengths[i])
		results[i].Data = make([]byte, lengths[i])
		copy(results[i].Data, resp[dataOffset:end])
		dataOffset = end
	}

	return results, nil
}

// sumReadFallback performs individual reads when no sum read command is supported.
func (conn *Session) sumReadFallback(requests []SumReadRequest) ([]SumReadResult, error) {
	results := make([]SumReadResult, len(requests))
	for i, req := range requests {
		data, err := conn.Read(req.Group, req.Offset, req.Length)
		if err != nil {
			results[i].Error = ReturnCodeDeviceError
			conn.logger.Warn("individual read failed in SumRead fallback", "error", err, "index", i)
		} else {
			results[i].Error = ReturnCodeNoErrors
			results[i].Data = data
		}
	}
	return results, nil
}

// SumWriteRequest represents a single write request within a sum/batch write.
type SumWriteRequest struct {
	Group  uint32
	Offset uint32
	Data   []byte
}

// SumWriteResult represents the result of a single write within a sum/batch write.
type SumWriteResult struct {
	Error ReturnCode
}

// SumWrite performs a batch write using GroupSumupWrite (0xF081).
// This writes multiple index group/offset combinations in a single ADS round-trip.
// If the sum command fails (e.g. on older PLCs), it falls back to individual writes.
//
// SumWrite is intentionally NOT migrated to executeSumCommand. The request payload
// has a header section (Group+Offset+Length per item) followed by a concatenated
// data section (variable-size Data bytes per item). The generic helper's fixed
// itemWriteSize cannot represent this. Custom orchestration is required.
func (conn *Session) SumWrite(requests []SumWriteRequest) ([]SumWriteResult, error) {
	if len(requests) == 0 {
		return nil, nil
	}

	// Skip SumWrite if we already know it's not supported.
	if conn.capabilities.SumWriteStateLoad() == 2 {
		return conn.sumWriteFallback(requests)
	}

	n := len(requests)

	// Build write data: N × 12 byte headers [Group(4) + Offset(4) + Length(4)] + concatenated data
	var totalDataLen int
	for _, req := range requests {
		totalDataLen += len(req.Data)
	}

	writeData := make([]byte, n*12+totalDataLen)
	for i, req := range requests {
		binary.LittleEndian.PutUint32(writeData[i*12:], req.Group)
		binary.LittleEndian.PutUint32(writeData[i*12+4:], req.Offset)
		binary.LittleEndian.PutUint32(writeData[i*12+8:], uint32(len(req.Data)))
	}
	dataOffset := n * 12
	for _, req := range requests {
		copy(writeData[dataOffset:], req.Data)
		dataOffset += len(req.Data)
	}

	// Response: N × 4 bytes (one uint32 error code per item)
	readLen := uint32(n * 4)

	resp, err := conn.WriteRead(uint32(GroupSumupWrite), uint32(n), readLen, writeData)
	if err != nil {
		if isSumCommandUnsupportedError(err) {
			conn.capabilities.SumWriteStateCAS(0, 2) // atomic: only first prober sets
			conn.logger.Warn("SumWrite not supported by PLC, using individual writes", "error", err)
			return conn.sumWriteFallback(requests)
		}
		// Don't fall back for transient errors — writes are not idempotent
		// and the PLC may have partially applied the batch
		return nil, fmt.Errorf("SumWrite failed: %w", err)
	}

	conn.capabilities.SumWriteStateCAS(0, 1) // atomic: only first prober sets

	if len(resp) < n*4 {
		return nil, fmt.Errorf("SumWrite response too short: got %d bytes, expected %d", len(resp), n*4)
	}

	results := make([]SumWriteResult, n)
	for i := 0; i < n; i++ {
		results[i].Error = ReturnCode(binary.LittleEndian.Uint32(resp[i*4:]))
	}

	return results, nil
}

// sumWriteFallback performs individual writes when sum write is not supported.
func (conn *Session) sumWriteFallback(requests []SumWriteRequest) ([]SumWriteResult, error) {
	results := make([]SumWriteResult, len(requests))
	for i, req := range requests {
		err := conn.Write(req.Group, req.Offset, req.Data)
		if err != nil {
			results[i].Error = ReturnCodeDeviceError
			conn.logger.Warn("individual write failed in SumWrite fallback", "error", err, "index", i)
		} else {
			results[i].Error = ReturnCodeNoErrors
		}
	}
	return results, nil
}

// SumNotificationRequest represents a single notification add request within a batch.
type SumNotificationRequest struct {
	Group            uint32
	Offset           uint32
	Length           uint32
	TransmissionMode TransMode
	MaxDelay         time.Duration
	CycleTime        time.Duration
}

// SumNotificationResult is a per-item result from SumAddDeviceNotification.
// Either Skipped is non-nil (library refused to send this entry — duplicate,
// resolution failure, transport-aborted batch) or Skipped is nil and Error
// carries the PLC-side return code. Handle is valid only when Skipped == nil
// AND Error == ReturnCodeNoErrors.
type SumNotificationResult struct {
	Handle  uint32
	Error   ReturnCode // PLC-side return code; valid only when Skipped == nil
	Skipped error      // non-nil if library skipped this entry; Error/Handle not meaningful
}

// SumAddDeviceNotification adds multiple device notifications in a single ADS
// round-trip using GroupSumupAddDeviceNotification (0xF085). Falls back to
// individual AddDeviceNotification calls on older PLCs.
func (conn *Session) SumAddDeviceNotification(requests []SumNotificationRequest) ([]SumNotificationResult, error) {
	spec := sumCmdSpec[SumNotificationRequest, SumNotificationResult]{
		stateLoad:             conn.capabilities.SumAddNotifStateLoad,
		stateCASToSupported:   func() bool { return conn.capabilities.SumAddNotifStateCAS(0, 1) },
		stateCASToUnsupported: func() bool { return conn.capabilities.SumAddNotifStateCAS(0, 2) },
		group:                 GroupSumupAddDeviceNotification,
		itemWriteSize:         40, // Group + Offset + Length + TransMode + MaxDelay + CycleTime + 16 reserved
		itemReadSize:          8,  // error(4) + handle(4)
		encode: func(buf []byte, req SumNotificationRequest) {
			binary.LittleEndian.PutUint32(buf[0:], req.Group)
			binary.LittleEndian.PutUint32(buf[4:], req.Offset)
			binary.LittleEndian.PutUint32(buf[8:], req.Length)
			binary.LittleEndian.PutUint32(buf[12:], uint32(req.TransmissionMode))
			binary.LittleEndian.PutUint32(buf[16:], uint32(req.MaxDelay.Nanoseconds()/100))
			binary.LittleEndian.PutUint32(buf[20:], uint32(req.CycleTime.Nanoseconds()/100))
			// bytes 24-39 reserved (already zero)
		},
		decode: func(resp []byte, n int) ([]SumNotificationResult, error) {
			items := make([]SumNotificationResult, n)
			for i := 0; i < n; i++ {
				items[i].Error = ReturnCode(binary.LittleEndian.Uint32(resp[i*8:]))
				items[i].Handle = binary.LittleEndian.Uint32(resp[i*8+4:])
			}
			return items, nil
		},
		fallback: conn.sumAddNotificationFallback,
	}
	return executeSumCommand(conn, spec, requests)
}

// SumDeleteDeviceNotification deletes multiple device notifications by handle
// in a single ADS round-trip using GroupSumupDeleteDeviceNotification (0xF086).
// Falls back to individual DeleteDeviceNotification calls on older PLCs.
func (conn *Session) SumDeleteDeviceNotification(handles []uint32) ([]ReturnCode, error) {
	spec := sumCmdSpec[uint32, ReturnCode]{
		stateLoad:             conn.capabilities.SumDeleteNotifStateLoad,
		stateCASToSupported:   func() bool { return conn.capabilities.SumDeleteNotifStateCAS(0, 1) },
		stateCASToUnsupported: func() bool { return conn.capabilities.SumDeleteNotifStateCAS(0, 2) },
		group:                 GroupSumupDeleteDeviceNotification,
		itemWriteSize:         4, // handle
		itemReadSize:          4, // error
		encode: func(buf []byte, h uint32) {
			binary.LittleEndian.PutUint32(buf, h)
		},
		decode: func(resp []byte, n int) ([]ReturnCode, error) {
			out := make([]ReturnCode, n)
			for i := 0; i < n; i++ {
				out[i] = ReturnCode(binary.LittleEndian.Uint32(resp[i*4:]))
			}
			return out, nil
		},
		fallback: conn.sumDeleteNotificationFallback,
	}
	errors, err := executeSumCommand(conn, spec, handles)
	if err != nil {
		return nil, err
	}
	if len(errors) == 0 {
		return errors, nil
	}

	// cleanup: remove activeNotifications entries for successful or
	// handle-invalid deletes. handle-invalid is success-equivalent — PLC
	// already considers the handle gone.
	conn.notifs.lock.Lock()
	for i, h := range handles {
		if errors[i] == ReturnCodeNoErrors || errors[i] == ReturnCodeDeviceNotifyHandleInvalid {
			if sym := conn.notifs.activeNotifications[h]; sym != nil {
				conn.removeNotificationConfig(sym.FullName)
			}
			delete(conn.notifs.activeNotifications, h)
			conn.logger.Info("batch deleted notification handle", "handle", h, "errorCode", uint32(errors[i]))
		}
	}
	if len(conn.notifs.activeNotifications) == 0 {
		conn.notifs.notificationChannel = nil
	}
	conn.notifs.lock.Unlock()

	return errors, nil
}

// sumAddNotificationFallback adds notifications individually when sum commands are not supported.
// It also downgrades v2 transmission modes to v1 equivalents since older PLCs silently ignore v2 modes.
func (conn *Session) sumAddNotificationFallback(requests []SumNotificationRequest) ([]SumNotificationResult, error) {
	results := make([]SumNotificationResult, len(requests))
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
			results[i].Error = ReturnCodeDeviceError
			conn.logger.Warn("individual AddDeviceNotification failed in fallback", "error", err, "index", i)
		} else {
			results[i].Handle = h
		}
	}
	return results, nil
}

// bestEffortDeleteNotifications attempts to delete the given handles via
// SumDeleteDeviceNotification. Errors are logged but never returned — this is
// for cleanup paths where the caller cannot meaningfully react to a failure
// (e.g. PLC unreachable during a reconnect retry). Returns the count of
// successfully deleted handles for logging. Treats ReturnCodeDeviceNotifyHandleInvalid
// as success-equivalent (handle already gone PLC-side).
func (conn *Session) bestEffortDeleteNotifications(handles []uint32) int {
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
	for _, code := range errors {
		if code == ReturnCodeNoErrors || code == ReturnCodeDeviceNotifyHandleInvalid {
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
func (conn *Session) sumDeleteNotificationFallback(handles []uint32) ([]ReturnCode, error) {
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
