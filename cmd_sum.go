package ads

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"time"
)

// Batched ADS sum commands: SumRead, SumWrite, SumAddDeviceNotification,
// SumDeleteDeviceNotification. Each falls back to individual commands when
// the PLC does not support the sum variant. Capability state is tracked via
// the capabilities type on *Client (see capabilities.go).

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
	encode                func(buf []byte, req Req) error
	decode                func(resp []byte, n int) ([]Res, error)
	fallback              func(ctx context.Context, requests []Req) ([]Res, error)
}

// executeSumCommand orchestrates a sum command end-to-end:
//  1. If state is "unsupported" (2), call fallback directly.
//  2. Encode requests into a contiguous buffer of n × itemWriteSize bytes.
//  3. Issue WriteRead.
//  4. On error: if isSumCommandUnsupportedError, CAS state to unsupported,
//     log Warn, call fallback. Otherwise propagate.
//  5. On success: CAS state to supported, length-check response, decode.
func executeSumCommand[Req any, Res any](ctx context.Context, c *Client, spec sumCmdSpec[Req, Res], requests []Req) ([]Res, error) {
	if len(requests) == 0 {
		return nil, nil
	}
	if spec.stateLoad() == 2 {
		return spec.fallback(ctx, requests)
	}
	n := len(requests)
	// Guard against uint32 overflow on response-size accounting. SumRead has the
	// same check at SumRead:128-130; mirror here for the generic executor so
	// SumAddDeviceNotification / SumDeleteDeviceNotification get the same defense.
	if uint64(n)*uint64(spec.itemReadSize) > uint64(math.MaxUint32) {
		return nil, fmt.Errorf("sum command (group 0x%X) response size overflow: %d items × %d bytes", uint32(spec.group), n, spec.itemReadSize)
	}
	if uint64(n)*uint64(spec.itemWriteSize) > uint64(math.MaxUint32) {
		return nil, fmt.Errorf("sum command (group 0x%X) request size overflow: %d items × %d bytes", uint32(spec.group), n, spec.itemWriteSize)
	}
	writeData := make([]byte, n*spec.itemWriteSize)
	for i, req := range requests {
		if err := spec.encode(writeData[i*spec.itemWriteSize:(i+1)*spec.itemWriteSize], req); err != nil {
			return nil, fmt.Errorf("encode item %d: %w", i, err)
		}
	}
	readLen := uint32(n * spec.itemReadSize)
	resp, err := c.WriteRead(ctx, uint32(spec.group), uint32(n), readLen, writeData)
	if err != nil {
		if isSumCommandUnsupportedError(err) {
			spec.stateCASToUnsupported()
			c.logger.Warn("sum command not supported by PLC, using fallback",
				"group", uint32(spec.group),
				"error", err)
			return spec.fallback(ctx, requests)
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
func (c *Client) SumRead(ctx context.Context, requests []SumReadRequest) ([]SumReadResult, error) {
	if len(requests) == 0 {
		return nil, nil
	}

	cmd := c.capabilities.SumReadCmdLoad()
	if cmd == 1 {
		// Already determined: no sum read support
		return c.sumReadFallback(ctx, requests)
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
		return c.sumReadExec(ctx, Group(cmd), uint32(n), readLen, writeData, requests)
	}

	// First call: probe starting with Ex2, then Ex
	return c.sumReadProbe(ctx, uint32(n), readLen, writeData, requests)
}

// sumReadProbe tries SumReadEx2 then SumReadEx, caching the first that works.
func (c *Client) sumReadProbe(ctx context.Context, count uint32, readLen uint32, writeData []byte, requests []SumReadRequest) ([]SumReadResult, error) {
	// Try SumReadEx2 (0xF084) first
	resp, err := c.WriteRead(ctx, uint32(GroupSumupReadEx2), count, readLen, writeData)
	if err == nil {
		c.capabilities.SumReadCmdStore(uint32(GroupSumupReadEx2))
		c.logger.Info("SumRead using SumReadEx2 (0xF084)")
		return c.parseSumReadResponse(resp, int(count), requests)
	}
	if !isSumCommandUnsupportedError(err) {
		return nil, fmt.Errorf("SumRead failed: %w", err)
	}
	c.logger.Info("SumReadEx2 not supported, trying SumReadEx", "error", err)

	// Try SumReadEx (0xF083)
	resp, err = c.WriteRead(ctx, uint32(GroupSumupReadEx), count, readLen, writeData)
	if err == nil {
		c.capabilities.SumReadCmdStore(uint32(GroupSumupReadEx))
		c.logger.Info("SumRead using SumReadEx (0xF083)")
		return c.parseSumReadResponse(resp, int(count), requests)
	}
	if !isSumCommandUnsupportedError(err) {
		return nil, fmt.Errorf("SumRead failed: %w", err)
	}
	c.logger.Warn("SumReadEx not supported, using individual reads", "error", err)

	// No sum read support
	c.capabilities.SumReadCmdStore(1)
	return c.sumReadFallback(ctx, requests)
}

// sumReadExec performs a sum read with a known-good command.
func (c *Client) sumReadExec(ctx context.Context, group Group, count uint32, readLen uint32, writeData []byte, requests []SumReadRequest) ([]SumReadResult, error) {
	resp, err := c.WriteRead(ctx, uint32(group), count, readLen, writeData)
	if err != nil {
		return nil, fmt.Errorf("SumRead failed: %w", err)
	}
	return c.parseSumReadResponse(resp, int(count), requests)
}

// parseSumReadResponse parses the [N × (error, length)][data] response format
// shared by both SumReadEx (0xF083) and SumReadEx2 (0xF084).
//
// Note: The official Beckhoff PDF shows 0xF083 with a separate error array (like 0xF080),
// but empirical testing on both TwinCAT 2 and TwinCAT 3 confirms 0xF083 returns the
// interleaved format identical to 0xF084. See PROTOCOL.md for details.
func (c *Client) parseSumReadResponse(resp []byte, n int, requests []SumReadRequest) ([]SumReadResult, error) {
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
				c.logger.Error("SumRead errored-item declared length exceeds remaining bytes — protocol drift",
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
			c.logger.Error("SumRead per-item undersize — protocol drift",
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
			c.logger.Error("SumRead per-item oversize — protocol drift or wire corruption",
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
			c.logger.Error("SumRead truncated — likely protocol corruption",
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
func (c *Client) sumReadFallback(ctx context.Context, requests []SumReadRequest) ([]SumReadResult, error) {
	results := make([]SumReadResult, len(requests))
	for i, req := range requests {
		data, err := c.Read(ctx, req.Group, req.Offset, req.Length)
		if err != nil {
			var rc ReturnCode
			if errors.As(err, &rc) {
				results[i].Error = rc
			} else {
				results[i].Error = ReturnCodeDeviceError
			}
			c.logger.Warn("individual read failed in SumRead fallback", "error", err, "index", i)
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
func (c *Client) SumWrite(ctx context.Context, requests []SumWriteRequest) ([]SumWriteResult, error) {
	if len(requests) == 0 {
		return nil, nil
	}

	// Skip SumWrite if we already know it's not supported.
	if c.capabilities.SumWriteStateLoad() == 2 {
		return c.sumWriteFallback(ctx, requests)
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

	resp, err := c.WriteRead(ctx, uint32(GroupSumupWrite), uint32(n), readLen, writeData)
	if err != nil {
		if isSumCommandUnsupportedError(err) {
			c.capabilities.SumWriteStateCAS(0, 2) // atomic: only first prober sets
			c.logger.Warn("SumWrite not supported by PLC, using individual writes", "error", err)
			return c.sumWriteFallback(ctx, requests)
		}
		// Don't fall back for transient errors — writes are not idempotent
		// and the PLC may have partially applied the batch
		return nil, fmt.Errorf("SumWrite failed: %w", err)
	}

	c.capabilities.SumWriteStateCAS(0, 1) // atomic: only first prober sets

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
func (c *Client) sumWriteFallback(ctx context.Context, requests []SumWriteRequest) ([]SumWriteResult, error) {
	results := make([]SumWriteResult, len(requests))
	for i, req := range requests {
		err := c.Write(ctx, req.Group, req.Offset, req.Data)
		if err != nil {
			var rc ReturnCode
			if errors.As(err, &rc) {
				results[i].Error = rc
			} else {
				results[i].Error = ReturnCodeDeviceError
			}
			c.logger.Warn("individual write failed in SumWrite fallback", "error", err, "index", i)
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
func (c *Client) SumAddDeviceNotification(ctx context.Context, requests []SumNotificationRequest) ([]SumNotificationResult, error) {
	return c.sumAddDeviceNotificationFunc(ctx, requests, nil)
}

// sumAddDeviceNotificationFunc is SumAddDeviceNotification with a per-item
// callback, invoked with each item's result as soon as that result is known.
//
// This exists for the PLC that rejects the sum command: the call then degrades
// to one AddDeviceNotification per request, and the PLC starts streaming each
// handle the moment it creates it. A caller that waits for the whole batch
// cannot recognise the early handles for the rest of the batch — on TC2 a
// 40-symbol batch leaves the first handle unaccounted for several hundred
// milliseconds. With onItem the caller binds each handle as its own Add
// returns, which narrows that window to one round-trip.
//
// onItem is called synchronously on the calling goroutine, exactly once per
// request, in request order: progressively on the fallback path, and after
// decode on the batched path (where the PLC created every handle before
// answering, so there is nothing to report early). Callers therefore get one
// commit path regardless of which path ran. The returned slice carries the
// same results.
func (c *Client) sumAddDeviceNotificationFunc(
	ctx context.Context,
	requests []SumNotificationRequest,
	onItem func(index int, res SumNotificationResult),
) ([]SumNotificationResult, error) {
	// Set by the fallback closure so the batched-path emit below doesn't
	// double-report items the fallback already reported.
	fellBack := false
	spec := sumCmdSpec[SumNotificationRequest, SumNotificationResult]{
		stateLoad:             c.capabilities.SumAddNotifStateLoad,
		stateCASToSupported:   func() bool { return c.capabilities.SumAddNotifStateCAS(0, 1) },
		stateCASToUnsupported: func() bool { return c.capabilities.SumAddNotifStateCAS(0, 2) },
		group:                 GroupSumupAddDeviceNotification,
		itemWriteSize:         40, // Group + Offset + Length + TransMode + MaxDelay + CycleTime + 16 reserved
		itemReadSize:          8,  // error(4) + handle(4)
		encode: func(buf []byte, req SumNotificationRequest) error {
			maxDelayTicks, err := durationToADSTicks(req.MaxDelay, "MaxDelay")
			if err != nil {
				return err
			}
			cycleTimeTicks, err := durationToADSTicks(req.CycleTime, "CycleTime")
			if err != nil {
				return err
			}
			binary.LittleEndian.PutUint32(buf[0:], req.Group)
			binary.LittleEndian.PutUint32(buf[4:], req.Offset)
			binary.LittleEndian.PutUint32(buf[8:], req.Length)
			binary.LittleEndian.PutUint32(buf[12:], uint32(req.TransmissionMode))
			binary.LittleEndian.PutUint32(buf[16:], maxDelayTicks)
			binary.LittleEndian.PutUint32(buf[20:], cycleTimeTicks)
			// bytes 24-39 reserved (already zero)
			return nil
		},
		decode: func(resp []byte, n int) ([]SumNotificationResult, error) {
			items := make([]SumNotificationResult, n)
			for i := 0; i < n; i++ {
				items[i].Error = ReturnCode(binary.LittleEndian.Uint32(resp[i*8:]))
				items[i].Handle = binary.LittleEndian.Uint32(resp[i*8+4:])
			}
			return items, nil
		},
		fallback: func(ctx context.Context, reqs []SumNotificationRequest) ([]SumNotificationResult, error) {
			fellBack = true
			return c.sumAddNotificationFallback(ctx, reqs, onItem)
		},
	}
	results, err := executeSumCommand(ctx, c, spec, requests)
	if err != nil || onItem == nil || fellBack {
		return results, err
	}
	for i, r := range results {
		onItem(i, r)
	}
	return results, nil
}

// SumDeleteDeviceNotification deletes multiple device notifications by handle
// in a single ADS round-trip using GroupSumupDeleteDeviceNotification (0xF086).
// Falls back to individual DeleteDeviceNotification calls on older PLCs.
//
// Returns the per-handle ReturnCode slice from the PLC. Persistent
// activeNotifications cleanup is the caller's responsibility; Session
// wraps this with its notifications.lock cleanup in Session.SumDeleteDeviceNotification.
func (c *Client) SumDeleteDeviceNotification(ctx context.Context, handles []uint32) ([]ReturnCode, error) {
	spec := sumCmdSpec[uint32, ReturnCode]{
		stateLoad:             c.capabilities.SumDeleteNotifStateLoad,
		stateCASToSupported:   func() bool { return c.capabilities.SumDeleteNotifStateCAS(0, 1) },
		stateCASToUnsupported: func() bool { return c.capabilities.SumDeleteNotifStateCAS(0, 2) },
		group:                 GroupSumupDeleteDeviceNotification,
		itemWriteSize:         4, // handle
		itemReadSize:          4, // error
		encode: func(buf []byte, h uint32) error {
			binary.LittleEndian.PutUint32(buf, h)
			return nil
		},
		decode: func(resp []byte, n int) ([]ReturnCode, error) {
			out := make([]ReturnCode, n)
			for i := 0; i < n; i++ {
				out[i] = ReturnCode(binary.LittleEndian.Uint32(resp[i*4:]))
			}
			return out, nil
		},
		fallback: c.sumDeleteNotificationFallback,
	}
	return executeSumCommand(ctx, c, spec, handles)
}

// sumAddNotificationFallback adds notifications individually when sum commands are not supported.
// It also downgrades v2 transmission modes to v1 equivalents since older PLCs silently ignore v2 modes.
//
// onItem, when non-nil, is called with each result as that Add returns — before
// the remaining requests are sent. The PLC is already streaming that handle by
// then, so reporting it now is what lets the caller bind it in time.
func (c *Client) sumAddNotificationFallback(ctx context.Context, requests []SumNotificationRequest, onItem func(int, SumNotificationResult)) ([]SumNotificationResult, error) {
	results := make([]SumNotificationResult, len(requests))
	for i, req := range requests {
		// Downgrade v2 modes for older PLCs that don't support them
		transMode := downgradeTransMode(req.TransmissionMode)
		if transMode != req.TransmissionMode {
			c.logger.Info("downgraded transmission mode for older PLC",
				"from", req.TransmissionMode.String(),
				"to", transMode.String(),
				"index", i)
		}
		h, err := c.AddDeviceNotification(ctx, req.Group, req.Offset, req.Length, transMode, req.MaxDelay, req.CycleTime)
		if err != nil {
			var rc ReturnCode
			if !errors.As(err, &rc) {
				// Not a PLC verdict — the transport or the context failed. Two
				// reasons to stop here rather than carry on: labelling a link
				// failure as a device error blames the PLC for something it never
				// said, and every remaining item would burn its own timeout
				// against a connection that is already gone.
				results[i].Skipped = fmt.Errorf("%w: %w", ErrNotificationTransportFailure, err)
				for j := i + 1; j < len(requests); j++ {
					results[j].Skipped = fmt.Errorf("%w: batch aborted at item %d", ErrNotificationTransportFailure, i)
				}
				if onItem != nil {
					for j := i; j < len(requests); j++ {
						onItem(j, results[j])
					}
				}
				c.logger.Warn("AddDeviceNotification fallback aborted: transport failed mid-batch",
					"error", err, "index", i, "abandoned", len(requests)-i)
				return results, err
			}
			results[i].Error = rc
			c.logger.Warn("individual AddDeviceNotification failed in fallback", "error", err, "index", i)
		} else {
			results[i].Handle = h
		}
		if onItem != nil {
			onItem(i, results[i])
		}
	}
	return results, nil
}

// bestEffortDeleteNotifications attempts to delete the given handles via
// SumDeleteDeviceNotification (Session wrapper, so notifications cleanup also
// fires). Errors are logged but never returned — this is for cleanup
// paths where the caller cannot meaningfully react to a failure (e.g.
// PLC unreachable during a reconnect retry). Returns the count of
// successfully deleted handles. Treats ReturnCodeDeviceNotifyHandleInvalid
// (0x714) and ReturnCodeDeviceClientUnknown (0x715) as success-equivalent
// (handle already gone PLC-side / client identity dropped post-reconnect)
// via isBestEffortDeleteSuccess.
//
// Lives on *Session because it routes through Session.SumDeleteDeviceNotification
// to keep activeNotifications consistent with the PLC.
func (sess *Session) bestEffortDeleteNotifications(ctx context.Context, handles []uint32) int {
	if len(handles) == 0 {
		return 0
	}
	// userTeardown=false: this is recovery cleanup, not a user releasing their
	// last subscription, so it must not clear notificationChannel — the
	// resubscribe that follows needs it. See sumDeleteDeviceNotification.
	errors, err := sess.sumDeleteDeviceNotification(ctx, handles, false)
	// Count successes from any returned codes — sumDeleteNotificationFallback
	// returns partial codes alongside a non-nil error when it short-circuits
	// on transport failure, so handles cleaned up before the failure are not
	// "lost" from the operator's perspective.
	deleted := 0
	for _, code := range errors {
		if isBestEffortDeleteSuccess(code) {
			deleted++
		}
	}
	if err != nil {
		sess.logger.Warn("bestEffortDelete: SumDeleteDeviceNotification failed",
			"error", err,
			"partial_deleted", deleted,
			"handles", len(handles))
		return deleted
	}
	if deleted < len(handles) {
		sess.logger.Warn("bestEffortDelete: some handles not cleaned up",
			"deleted", deleted,
			"requested", len(handles))
	}
	return deleted
}

// sumDeleteNotificationFallback deletes notifications individually when sum commands are not supported.
// Two distinct error paths:
//
//   - PLC returned an ADS-level ReturnCode (handle-invalid, client-unknown,
//     device-error, etc.) → stored in codes[i], loop continues. Caller's
//     isBestEffortDeleteSuccess() reduces success codes.
//   - Non-ReturnCode error (transport closed, ctx canceled, marshaling
//     failure) → loop SHORT-CIRCUITS and returns the partial codes plus
//     the wrapped error. Subsequent handles would hit the same transport
//     condition; pushing on would only multiply the failure log spam and
//     mask the root cause behind synthesized ReturnCodeDeviceError values.
func (c *Client) sumDeleteNotificationFallback(ctx context.Context, handles []uint32) ([]ReturnCode, error) {
	codes := make([]ReturnCode, len(handles))
	for i, h := range handles {
		err := c.DeleteDeviceNotification(ctx, h)
		if err == nil {
			codes[i] = ReturnCodeNoErrors
			continue
		}
		var rc ReturnCode
		if !errors.As(err, &rc) {
			// Transport / ctx / non-ADS failure: don't synthesize a
			// ReturnCode that would lie about the cause downstream. Short-
			// circuit with the partial codes processed so far.
			c.logger.Warn("individual DeleteDeviceNotification: transport-level failure, aborting fallback",
				"error", err, "handle", h, "processed", i, "remaining", len(handles)-i)
			return codes[:i], fmt.Errorf("sumDeleteNotificationFallback aborted at handle index %d: %w", i, err)
		}
		codes[i] = rc
		// 0x714 / 0x715 = handle already gone PLC-side (typical after
		// reconnect or in best-effort cleanup paths). Log Debug; the
		// caller's reduction via isBestEffortDeleteSuccess still counts
		// these as cleanup wins. Other codes = real failure → Warn.
		if isBestEffortDeleteSuccess(codes[i]) {
			c.logger.Debug("individual DeleteDeviceNotification: handle already gone PLC-side", "error", err, "handle", h, "code", codes[i])
		} else {
			c.logger.Warn("individual DeleteDeviceNotification failed in fallback", "error", err, "handle", h, "code", codes[i])
		}
	}
	return codes, nil
}
