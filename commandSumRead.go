package ads

import (
	"encoding/binary"
	"fmt"

	"github.com/rs/zerolog/log"
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

// SumRead performs a batch read using GroupSumupReadEx2 (0xF084).
// This reads multiple index group/offset/length combinations in a single ADS round-trip.
// If the sum command fails (e.g. on older PLCs), it falls back to individual reads.
func (conn *Connection) SumRead(requests []SumReadRequest) ([]SumReadResult, error) {
	if len(requests) == 0 {
		return nil, nil
	}

	// Skip SumRead if we already know it's not supported
	if conn.sumReadChecked.Load() && !conn.sumReadSupported.Load() {
		return conn.sumReadFallback(requests)
	}

	n := len(requests)

	// Build write data: N × 12 bytes (group + offset + length per request)
	writeData := make([]byte, n*12)
	var totalReadLen uint32
	for i, req := range requests {
		binary.LittleEndian.PutUint32(writeData[i*12:], req.Group)
		binary.LittleEndian.PutUint32(writeData[i*12+4:], req.Offset)
		binary.LittleEndian.PutUint32(writeData[i*12+8:], req.Length)
		totalReadLen += req.Length
	}

	// Read data: N × 4 bytes error codes + N × 4 bytes actual lengths + concatenated data
	readLen := uint32(n*4+n*4) + totalReadLen

	resp, err := conn.WriteRead(uint32(GroupSumupReadEx2), uint32(n), readLen, writeData)
	if err != nil {
		if !conn.sumReadChecked.Load() {
			log.Warn().Err(err).Msg("SumRead not supported by PLC, using individual reads")
			conn.sumReadSupported.Store(false)
			conn.sumReadChecked.Store(true)
		}
		return conn.sumReadFallback(requests)
	}

	if !conn.sumReadChecked.Load() {
		conn.sumReadSupported.Store(true)
		conn.sumReadChecked.Store(true)
	}

	if len(resp) < n*8 {
		return nil, fmt.Errorf("SumRead response too short: got %d bytes, expected at least %d", len(resp), n*8)
	}

	results := make([]SumReadResult, n)

	// Parse error codes (N × 4 bytes)
	for i := 0; i < n; i++ {
		results[i].Error = ReturnCode(binary.LittleEndian.Uint32(resp[i*4:]))
	}

	// Parse actual lengths (N × 4 bytes)
	lengths := make([]uint32, n)
	for i := 0; i < n; i++ {
		lengths[i] = binary.LittleEndian.Uint32(resp[n*4+i*4:])
	}

	// Parse concatenated data
	dataOffset := n * 8
	for i := 0; i < n; i++ {
		end := dataOffset + int(lengths[i])
		if end > len(resp) {
			results[i].Error = ReturnCodeDeviceInvalidSize
			continue
		}
		results[i].Data = make([]byte, lengths[i])
		copy(results[i].Data, resp[dataOffset:end])
		dataOffset = end
	}

	return results, nil
}

// sumReadFallback performs individual reads when sum read is not supported.
func (conn *Connection) sumReadFallback(requests []SumReadRequest) ([]SumReadResult, error) {
	results := make([]SumReadResult, len(requests))
	for i, req := range requests {
		data, err := conn.Read(req.Group, req.Offset, req.Length)
		if err != nil {
			results[i].Error = ReturnCodeDeviceError
			log.Warn().Err(err).Int("index", i).Msg("individual read failed in SumRead fallback")
		} else {
			results[i].Error = ReturnCodeNoErrors
			results[i].Data = data
		}
	}
	return results, nil
}
