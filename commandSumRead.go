package ads

import (
	"encoding/binary"
	"fmt"
	"math"
)

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
func (conn *Connection) SumRead(requests []SumReadRequest) ([]SumReadResult, error) {
	if len(requests) == 0 {
		return nil, nil
	}

	cmd := conn.sumReadCmd.Load()
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
func (conn *Connection) sumReadProbe(count uint32, readLen uint32, writeData []byte, requests []SumReadRequest) ([]SumReadResult, error) {
	// Try SumReadEx2 (0xF084) first
	resp, err := conn.WriteRead(uint32(GroupSumupReadEx2), count, readLen, writeData)
	if err == nil {
		conn.sumReadCmd.Store(uint32(GroupSumupReadEx2))
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
		conn.sumReadCmd.Store(uint32(GroupSumupReadEx))
		conn.logger.Info("SumRead using SumReadEx (0xF083)")
		return conn.parseSumReadResponse(resp, int(count), requests)
	}
	if !isSumCommandUnsupportedError(err) {
		return nil, fmt.Errorf("SumRead failed: %w", err)
	}
	conn.logger.Warn("SumReadEx not supported, using individual reads", "error", err)

	// No sum read support
	conn.sumReadCmd.Store(1)
	return conn.sumReadFallback(requests)
}

// sumReadExec performs a sum read with a known-good command.
func (conn *Connection) sumReadExec(group Group, count uint32, readLen uint32, writeData []byte, requests []SumReadRequest) ([]SumReadResult, error) {
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
func (conn *Connection) parseSumReadResponse(resp []byte, n int, requests []SumReadRequest) ([]SumReadResult, error) {
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
		// Compare in uint32 space first to avoid int wrap on 32-bit Go (F-09).
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
func (conn *Connection) sumReadFallback(requests []SumReadRequest) ([]SumReadResult, error) {
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
