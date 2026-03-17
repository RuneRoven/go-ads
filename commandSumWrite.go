package ads

import (
	"encoding/binary"
	"fmt"

	"github.com/rs/zerolog/log"
)

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
func (conn *Connection) SumWrite(requests []SumWriteRequest) ([]SumWriteResult, error) {
	if len(requests) == 0 {
		return nil, nil
	}

	// Skip SumWrite if we already know sum commands are not supported.
	// We reuse sumReadChecked/sumReadSupported because PLCs that lack SumRead
	// (GroupSumupRead 0xF080) also lack SumWrite (GroupSumupWrite 0xF081) —
	// both were introduced together in the ADS sum command extension.
	if conn.sumReadChecked.Load() && !conn.sumReadSupported.Load() {
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
		if !conn.sumReadChecked.Load() {
			log.Warn().Err(err).Msg("SumWrite not supported by PLC, using individual writes")
			conn.sumReadSupported.Store(false)
			conn.sumReadChecked.Store(true)
		}
		return conn.sumWriteFallback(requests)
	}

	if !conn.sumReadChecked.Load() {
		conn.sumReadSupported.Store(true)
		conn.sumReadChecked.Store(true)
	}

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
func (conn *Connection) sumWriteFallback(requests []SumWriteRequest) ([]SumWriteResult, error) {
	results := make([]SumWriteResult, len(requests))
	for i, req := range requests {
		err := conn.Write(req.Group, req.Offset, req.Data)
		if err != nil {
			results[i].Error = ReturnCodeDeviceError
			log.Warn().Err(err).Int("index", i).Msg("individual write failed in SumWrite fallback")
		} else {
			results[i].Error = ReturnCodeNoErrors
		}
	}
	return results, nil
}
