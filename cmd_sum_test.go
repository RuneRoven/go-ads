package ads

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log/slog"
	"math"
	"strings"
	"sync"
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
// Validates: R-SUM-006.
func TestParseSumReadResponse_LengthOverflow(t *testing.T) {
	conn := &Session{logger: getDefaultLogger()}
	conn.client = &Client{logger: conn.logger}

	resp := craftSumReadResponse(
		[]ReturnCode{ReturnCodeNoErrors},
		[]uint32{0xFFFFFFFE},
		[]byte{0x01, 0x02, 0x03, 0x04},
	)
	requests := []SumReadRequest{{Length: 4}}

	results, err := conn.client.parseSumReadResponse(resp, 1, requests)
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
// Validates: R-SUM-006.
func TestParseSumReadResponse_TruncationLogsError(t *testing.T) {
	var logBuf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logBuf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	conn := &Session{logger: logger}
	conn.client = &Client{logger: logger}

	// Two items: first declares length=8, second declares length=4. Data section
	// has only 4 bytes total — first item alone exceeds remaining bytes.
	resp := craftSumReadResponse(
		[]ReturnCode{ReturnCodeNoErrors, ReturnCodeNoErrors},
		[]uint32{8, 4},
		[]byte{0x01, 0x02, 0x03, 0x04},
	)
	requests := []SumReadRequest{{Length: 8}, {Length: 4}}

	results, err := conn.client.parseSumReadResponse(resp, 2, requests)
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

// F-11: PLC oversizing one item's response shifts later items' offsets.
// Defense: reject when lengths[i] > requests[i].Length even if total bytes
// fit in the response.
// Validates: R-SUM-006.
func TestParseSumReadResponse_PerItemOversize(t *testing.T) {
	var logBuf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logBuf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	conn := &Session{logger: logger}
	conn.client = &Client{logger: logger}

	// Two items each requested as 4 bytes, but item 0 declares 8 bytes returned.
	// Total response size accommodates 8+4=12 data bytes so the gross truncation
	// guard (F-09) does NOT fire; only the per-item check (F-11) catches it.
	resp := craftSumReadResponse(
		[]ReturnCode{ReturnCodeNoErrors, ReturnCodeNoErrors},
		[]uint32{8, 4},
		[]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12},
	)
	requests := []SumReadRequest{{Length: 4}, {Length: 4}}

	results, err := conn.client.parseSumReadResponse(resp, 2, requests)
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
	if !strings.Contains(logOut, "per-item oversize") {
		t.Errorf("expected per-item oversize log, got: %s", logOut)
	}
}

// bestEffortDeleteNotifications returns 0 for an empty input slice and never
// touches the network.
// Validates: R-NOT-015.
func TestBestEffortDeleteNotifications_Empty(t *testing.T) {
	conn := &Session{logger: getDefaultLogger()}
	conn.client = &Client{logger: conn.logger}
	got := conn.bestEffortDeleteNotifications(nil)
	if got != 0 {
		t.Errorf("expected 0, got %d", got)
	}
	got = conn.bestEffortDeleteNotifications([]uint32{})
	if got != 0 {
		t.Errorf("expected 0, got %d", got)
	}
}

// --- SumRead overflow guard ---

// TestSumReadOverflowGuard exercises the production guard in
// Client.SumRead that rejects request slices whose summed Length plus
// per-item header overhead would overflow uint32. Drives the production
// code path with a synthetic two-request slice that crosses MaxUint32
// and asserts SumRead returns the documented error.
//
// Validates: R-SUM-006 (data-section integrity).
func TestSumReadOverflowGuard(t *testing.T) {
	// Build a Client without a live transport. SumRead's overflow check
	// runs before any wire I/O, so we never reach sendRequest. The
	// capabilities zero-value (sumReadCmd == 0) routes the call through
	// the probe path, which still computes totalReadLen first.
	c := &Client{
		logger: getDefaultLogger(),
		tx: &transport{
			sendChannel:    make(chan []byte),
			systemResponse: make(chan []byte),
			recvQueue:      make(chan []byte),
			activeRequests: map[uint32]chan []byte{},
		},
	}
	requests := []SumReadRequest{
		{Group: 1, Offset: 0, Length: math.MaxUint32},
		{Group: 1, Offset: 0, Length: 1}, // total > MaxUint32
	}

	_, err := c.SumRead(requests)
	if err == nil {
		t.Fatal("expected overflow error, got nil")
	}
	if !strings.Contains(err.Error(), "exceeds uint32 max") {
		t.Errorf("expected 'exceeds uint32 max' error, got: %v", err)
	}
}

// --- isSumCommandUnsupportedError ---

// Validates: R-SUM-001/R-SUM-002 (partial).
func TestIsSumCommandUnsupportedError(t *testing.T) {
	tests := []struct {
		err  error
		want bool
	}{
		{ReturnCodeDeviceServiceNotSupported, true},
		{ReturnCodeGlobalUnknownCommandID, true},
		{ReturnCodeGlobalUnknownAdsCommand, true},
		{ReturnCodeDeviceBusy, false},
		{ReturnCodeDeviceTimeout, false},
		{fmt.Errorf("network error"), false},
		{nil, false},
	}
	for _, tt := range tests {
		got := isSumCommandUnsupportedError(tt.err)
		if got != tt.want {
			t.Errorf("isSumCommandUnsupportedError(%v) = %v, want %v", tt.err, got, tt.want)
		}
	}
}

// --- Sum probe state CAS ---

// Validates: R-SUM-003.
func TestSumProbeStateTransitions(t *testing.T) {
	// Verify CAS 0→1 and 0→2 work, and second CAS is rejected
	conn := Session{client: &Client{}}

	// sumWriteState: 0→1
	if !conn.client.capabilities.SumWriteStateCAS(0, 1) {
		t.Error("CAS 0→1 should succeed")
	}
	if conn.client.capabilities.SumWriteStateLoad() != 1 {
		t.Error("state should be 1")
	}
	// Second goroutine trying 0→2 should fail
	if conn.client.capabilities.SumWriteStateCAS(0, 2) {
		t.Error("CAS 0→2 should fail when state is 1")
	}

	// sumAddNotifState: 0→2
	if !conn.client.capabilities.SumAddNotifStateCAS(0, 2) {
		t.Error("CAS 0→2 should succeed")
	}
	if conn.client.capabilities.SumAddNotifStateLoad() != 2 {
		t.Error("state should be 2")
	}
	// sumDeleteNotifState independent of sumAddNotifState
	if conn.client.capabilities.SumDeleteNotifStateLoad() != 0 {
		t.Error("delete state should still be 0 (independent of add)")
	}

	// Reset works
	conn.client.capabilities.SumWriteStateStore(0)
	if conn.client.capabilities.SumWriteStateLoad() != 0 {
		t.Error("reset should set state to 0")
	}
}

// Validates: R-SUM-003 / R-LOCK-003.
func TestSumProbeStateConcurrent(t *testing.T) {
	// Concurrent CAS: only one goroutine should win
	conn := Session{client: &Client{}}
	const goroutines = 100
	var wg sync.WaitGroup
	wins := make(chan uint32, goroutines)

	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func(id int) {
			defer wg.Done()
			val := uint32(1)
			if id%2 == 0 {
				val = 2
			}
			if conn.client.capabilities.SumWriteStateCAS(0, val) {
				wins <- val
			}
		}(i)
	}
	wg.Wait()
	close(wins)

	count := 0
	for range wins {
		count++
	}
	if count != 1 {
		t.Errorf("expected exactly 1 CAS winner, got %d", count)
	}
}
