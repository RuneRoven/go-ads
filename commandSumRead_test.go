package ads

import (
	"bytes"
	"encoding/binary"
	"log/slog"
	"strings"
	"testing"
)

// craftSumReadResponse builds a byte response in the [N×(error,length)][data] format.
// errors[i] and dataLengths[i] go in the header section; data is the concatenated payload.
func craftSumReadResponse(errs []ReturnCode, dataLengths []uint32, data []byte) []byte {
	n := len(errs)
	buf := make([]byte, n*8+len(data))
	for i := 0; i < n; i++ {
		binary.LittleEndian.PutUint32(buf[i*8:], uint32(errs[i]))
		binary.LittleEndian.PutUint32(buf[i*8+4:], dataLengths[i])
	}
	copy(buf[n*8:], data)
	return buf
}

// F-09: a malicious / buggy PLC sending lengths[i] = 0xFFFFFFFE must not cause
// a negative int cast (32-bit Go) or huge make() allocation.
// On 64-bit Go this is defense-in-depth; on 32-bit it is a real bug.
func TestParseSumReadResponse_LengthOverflow(t *testing.T) {
	conn := &Connection{logger: getDefaultLogger()}

	resp := craftSumReadResponse(
		[]ReturnCode{ReturnCodeNoErrors},
		[]uint32{0xFFFFFFFE},
		[]byte{0x01, 0x02, 0x03, 0x04},
	)
	requests := []SumReadRequest{{Length: 4}}

	results, err := conn.parseSumReadResponse(resp, 1, requests)
	if err != nil {
		t.Fatalf("unexpected outer error: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if results[0].Error != ReturnCodeDeviceInvalidSize {
		t.Errorf("expected ReturnCodeDeviceInvalidSize, got %v", results[0].Error)
	}
}

// F-10: truncated response must emit an Error log so wire corruption is
// distinguishable from genuine PLC errors.
func TestParseSumReadResponse_TruncationLogsError(t *testing.T) {
	var logBuf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logBuf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	conn := &Connection{logger: logger}

	// Two items: first declares length=8, second declares length=4. Data section
	// has only 4 bytes total — first item alone exceeds remaining bytes.
	resp := craftSumReadResponse(
		[]ReturnCode{ReturnCodeNoErrors, ReturnCodeNoErrors},
		[]uint32{8, 4},
		[]byte{0x01, 0x02, 0x03, 0x04},
	)
	requests := []SumReadRequest{{Length: 8}, {Length: 4}}

	results, err := conn.parseSumReadResponse(resp, 2, requests)
	if err != nil {
		t.Fatalf("unexpected outer error: %v", err)
	}
	if results[0].Error != ReturnCodeDeviceInvalidSize {
		t.Errorf("results[0].Error = %v, want ReturnCodeDeviceInvalidSize", results[0].Error)
	}
	if results[1].Error != ReturnCodeDeviceInvalidSize {
		t.Errorf("results[1].Error = %v, want ReturnCodeDeviceInvalidSize", results[1].Error)
	}

	logOut := logBuf.String()
	if !strings.Contains(logOut, "SumRead truncated") {
		t.Errorf("expected truncation log, got: %s", logOut)
	}
}
