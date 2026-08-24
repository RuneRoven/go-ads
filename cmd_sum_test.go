package ads

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"log/slog"
	"math"
	"strings"
	"sync"
	"testing"
	"time"
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
	conn.client.Store(&Client{logger: conn.logger})

	resp := craftSumReadResponse(
		[]ReturnCode{ReturnCodeNoErrors},
		[]uint32{0xFFFFFFFE},
		[]byte{0x01, 0x02, 0x03, 0x04},
	)
	requests := []SumReadRequest{{Length: 4}}

	results, err := conn.client.Load().parseSumReadResponse(resp, 1, requests)
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
	conn.client.Store(&Client{logger: logger})

	// Two items: first declares length=8, second declares length=4. Data section
	// has only 4 bytes total — first item alone exceeds remaining bytes.
	resp := craftSumReadResponse(
		[]ReturnCode{ReturnCodeNoErrors, ReturnCodeNoErrors},
		[]uint32{8, 4},
		[]byte{0x01, 0x02, 0x03, 0x04},
	)
	requests := []SumReadRequest{{Length: 8}, {Length: 4}}

	results, err := conn.client.Load().parseSumReadResponse(resp, 2, requests)
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
	conn.client.Store(&Client{logger: logger})

	// Two items each requested as 4 bytes, but item 0 declares 8 bytes returned.
	// Total response size accommodates 8+4=12 data bytes so the gross truncation
	// guard (F-09) does NOT fire; only the per-item check (F-11) catches it.
	resp := craftSumReadResponse(
		[]ReturnCode{ReturnCodeNoErrors, ReturnCodeNoErrors},
		[]uint32{8, 4},
		[]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12},
	)
	requests := []SumReadRequest{{Length: 4}, {Length: 4}}

	results, err := conn.client.Load().parseSumReadResponse(resp, 2, requests)
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
	conn.client.Store(&Client{logger: conn.logger})
	got := conn.bestEffortDeleteNotifications(context.Background(), nil)
	if got != 0 {
		t.Errorf("expected 0, got %d", got)
	}
	got = conn.bestEffortDeleteNotifications(context.Background(), []uint32{})
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
			activeRequests: map[uint32]chan amsReply{},
		},
	}
	requests := []SumReadRequest{
		{Group: 1, Offset: 0, Length: math.MaxUint32},
		{Group: 1, Offset: 0, Length: 1}, // total > MaxUint32
	}

	_, err := c.SumRead(context.Background(), requests)
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
	conn := Session{}
	conn.client.Store(&Client{})

	// sumWriteState: 0→1
	if !conn.client.Load().capabilities.SumWriteStateCAS(0, 1) {
		t.Error("CAS 0→1 should succeed")
	}
	if conn.client.Load().capabilities.SumWriteStateLoad() != 1 {
		t.Error("state should be 1")
	}
	// Second goroutine trying 0→2 should fail
	if conn.client.Load().capabilities.SumWriteStateCAS(0, 2) {
		t.Error("CAS 0→2 should fail when state is 1")
	}

	// sumAddNotifState: 0→2
	if !conn.client.Load().capabilities.SumAddNotifStateCAS(0, 2) {
		t.Error("CAS 0→2 should succeed")
	}
	if conn.client.Load().capabilities.SumAddNotifStateLoad() != 2 {
		t.Error("state should be 2")
	}
	// sumDeleteNotifState independent of sumAddNotifState
	if conn.client.Load().capabilities.SumDeleteNotifStateLoad() != 0 {
		t.Error("delete state should still be 0 (independent of add)")
	}

	// Reset works
	conn.client.Load().capabilities.SumWriteStateStore(0)
	if conn.client.Load().capabilities.SumWriteStateLoad() != 0 {
		t.Error("reset should set state to 0")
	}
}

// Validates: R-SUM-003 / R-LOCK-003.
func TestSumProbeStateConcurrent(t *testing.T) {
	// Concurrent CAS: only one goroutine should win
	conn := Session{}
	conn.client.Store(&Client{})
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
			if conn.client.Load().capabilities.SumWriteStateCAS(0, val) {
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

// seedSymbol installs a minimal cache symbol entry so getSymbol short-circuits.
// Length=1 + DataType="BOOL" parses cleanly; Handle is non-zero so getSymbol
// skips the PLC GetHandleByName roundtrip.
func seedSymbol(sess *Session, name string, handle uint32) {
	sym := &symbol{
		FullName: name,
		Handle:   handle,
		Length:   1,
		DataType: "BOOL",
	}
	sess.cache.symbols[symbolKey(name)] = sym
}

// TestSession_ReadMultipleSymbols_StaleDetection validates R-CACHE-009
// detection in the sum-batch decode path: a per-item ReturnCode in the
// stale-cache set (e.g. 0x711 SymbolVersionInvalid) must trigger
// handleStaleDetection through the configured strategy callback even when
// the batched roundtrip itself succeeded.
//
// Validates: R-CACHE-009 (sum-batch per-item detection wiring).
func TestSession_ReadMultipleSymbols_StaleDetection(t *testing.T) {
	srv := startScriptableServer(t)
	defer srv.stop()

	cbReason := make(chan Reason, 1)
	sess, _ := newWiredTestSession(t, srv,
		WithSymbolVersionStrategy(SymbolVersionIgnore),
		WithOnSymbolVersionChanged(func(r Reason) {
			select {
			case cbReason <- r:
			default:
			}
		}),
	)
	seedSymbol(sess, "MAIN.a", 0x1001)
	seedSymbol(sess, "MAIN.b", 0x1002)

	// SumReadEx2 (0xF084) handler: respond with one stale code + one OK.
	// Response shape: [N × (error(4), length(4))][data].
	srv.onWriteRead(GroupSumupReadEx2, func(_ []byte) []byte {
		resp := craftSumReadResponse(
			[]ReturnCode{ReturnCodeDeviceSymbolVersionInvalid, ReturnCodeNoErrors},
			[]uint32{0, 1},
			[]byte{0x00},
		)
		return resp
	})

	_, err := sess.ReadMultipleSymbols(context.Background(), []string{"MAIN.a", "MAIN.b"})
	if err != nil {
		t.Fatalf("ReadMultipleSymbols: %v", err)
	}

	select {
	case got := <-cbReason:
		if got != ReasonSymbolVersionInvalid {
			t.Errorf("callback reason = %q, want %q", got, ReasonSymbolVersionInvalid)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("strategy callback did not fire within 2s for sum-batch stale code")
	}
}

// TestSession_ReadMultipleSymbols_FiresCallbackOncePerBatch validates that
// when N>1 items in a single batched response carry stale codes, the
// strategy callback fires exactly ONCE (R-SES-011 "once per detection").
// Verified via a buffered channel of size 1 and a drain check after a
// short settle window.
//
// Validates: R-CACHE-009 + R-SES-011 (no callback amplification on batched ops).
func TestSession_ReadMultipleSymbols_FiresCallbackOncePerBatch(t *testing.T) {
	srv := startScriptableServer(t)
	defer srv.stop()

	// Buffer of 4 so a buggy implementation (one callback per stale item)
	// would visibly fill the channel; correct impl pushes exactly 1.
	calls := make(chan Reason, 4)
	sess, _ := newWiredTestSession(t, srv,
		WithSymbolVersionStrategy(SymbolVersionIgnore),
		WithOnSymbolVersionChanged(func(r Reason) { calls <- r }),
	)
	seedSymbol(sess, "MAIN.a", 0x2001)
	seedSymbol(sess, "MAIN.b", 0x2002)
	seedSymbol(sess, "MAIN.c", 0x2003)

	// Three stale codes — implementation must break after first.
	srv.onWriteRead(GroupSumupReadEx2, func(_ []byte) []byte {
		return craftSumReadResponse(
			[]ReturnCode{
				ReturnCodeDeviceSymbolVersionInvalid,
				ReturnCodeDeviceSymbolVersionInvalid,
				ReturnCodeDeviceSymbolNoFound,
			},
			[]uint32{0, 0, 0},
			nil,
		)
	})

	_, err := sess.ReadMultipleSymbols(context.Background(), []string{"MAIN.a", "MAIN.b", "MAIN.c"})
	if err != nil {
		t.Fatalf("ReadMultipleSymbols: %v", err)
	}

	// Wait for first callback.
	select {
	case <-calls:
	case <-time.After(2 * time.Second):
		t.Fatal("callback did not fire at all within 2s")
	}

	// Settle window: any extra (buggy-amplified) callbacks would surface here.
	select {
	case extra := <-calls:
		t.Fatalf("callback fired more than once per batch (got extra %q)", extra)
	case <-time.After(150 * time.Millisecond):
		// pass — no amplification
	}
}

// TestSession_WriteMultipleSymbols_StaleDetection validates R-CACHE-009
// detection in the SumWrite batch decode: a stale per-item code triggers
// the strategy callback. SumWrite response is [N × error(4)] with no data
// section, so this also exercises the bare-error-array decode path.
//
// Validates: R-CACHE-009 (SumWrite batch wiring).
func TestSession_WriteMultipleSymbols_StaleDetection(t *testing.T) {
	srv := startScriptableServer(t)
	defer srv.stop()

	cbReason := make(chan Reason, 1)
	sess, _ := newWiredTestSession(t, srv,
		WithSymbolVersionStrategy(SymbolVersionIgnore),
		WithOnSymbolVersionChanged(func(r Reason) {
			select {
			case cbReason <- r:
			default:
			}
		}),
	)
	seedSymbol(sess, "MAIN.a", 0x3001)
	seedSymbol(sess, "MAIN.b", 0x3002)

	// SumWrite (0xF081) response: N × uint32 per-item error codes.
	srv.onWriteRead(GroupSumupWrite, func(_ []byte) []byte {
		buf := make([]byte, 8)
		binary.LittleEndian.PutUint32(buf[0:], uint32(ReturnCodeDeviceSymbolVersionInvalid))
		binary.LittleEndian.PutUint32(buf[4:], uint32(ReturnCodeNoErrors))
		return buf
	})

	_, err := sess.WriteMultipleSymbols(context.Background(), map[string]string{
		"MAIN.a": "true",
		"MAIN.b": "false",
	})
	if err != nil {
		t.Fatalf("WriteMultipleSymbols: %v", err)
	}

	select {
	case got := <-cbReason:
		if got != ReasonSymbolVersionInvalid {
			t.Errorf("callback reason = %q, want %q", got, ReasonSymbolVersionInvalid)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("strategy callback did not fire within 2s for SumWrite stale code")
	}
}

// TestSumReadFallback_PreservesADSReturnCode validates that sumReadFallback
// propagates the real ADS ReturnCode from c.Read rather than masking it
// with ReturnCodeDeviceError. This is required so that
// readMultipleSymbolsRetry can detect stale-cache codes (e.g.
// ReturnCodeDeviceSymbolVersionInvalid) in per-item results.
//
// Validates: fallback error preservation (fix for sumReadFallback masking).
func TestSumReadFallback_PreservesADSReturnCode(t *testing.T) {
	srv := startScriptableServer(t)
	defer srv.stop()

	// Register a Read handler for an arbitrary group that returns the stale code.
	const testGroup Group = 0xABCD1234
	srv.onRead(testGroup, func(_, _, _ uint32) (ReturnCode, []byte) {
		return ReturnCodeDeviceSymbolVersionInvalid, nil
	})

	c, err := Dial(srv.host, srv.port, AMSAddress{}, AMSAddress{}, 2*time.Second)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer c.Close()

	reqs := []SumReadRequest{{Group: uint32(testGroup), Offset: 0, Length: 4}}
	results, err := c.sumReadFallback(context.Background(), reqs)
	if err != nil {
		t.Fatalf("sumReadFallback returned unexpected top-level error: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if results[0].Error != ReturnCodeDeviceSymbolVersionInvalid {
		t.Errorf("results[0].Error = %v, want ReturnCodeDeviceSymbolVersionInvalid", results[0].Error)
	}
}

// TestParseSumReadResponse_ErroredItemAdvancesOffset pins the deliberate
// behaviour that an errored item still consumes its declared length in the
// response data section so subsequent items remain aligned. Without this,
// a successful follow-up item parses the wrong bytes and silently corrupts
// the caller's data. A regression that skips dataOffset += lengths[i] for
// errored items would break this test on item 2's payload comparison.
func TestParseSumReadResponse_ErroredItemAdvancesOffset(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil))
	conn := &Session{logger: logger}
	conn.client.Store(&Client{logger: logger})

	// Three items: OK(4 bytes), ERR(declared 4 bytes), OK(4 bytes).
	// If alignment regresses, item 2 reads bytes 4..7 (item 1's payload)
	// instead of bytes 8..11.
	item1 := []byte{0xAA, 0xAA, 0xAA, 0xAA}
	item1Err := []byte{0xBB, 0xBB, 0xBB, 0xBB} // PLC may or may not send these
	item2 := []byte{0xCC, 0xCC, 0xCC, 0xCC}
	data := append(append(append([]byte{}, item1...), item1Err...), item2...)

	resp := craftSumReadResponse(
		[]ReturnCode{ReturnCodeNoErrors, ReturnCodeDeviceSymbolVersionInvalid, ReturnCodeNoErrors},
		[]uint32{4, 4, 4},
		data,
	)
	requests := []SumReadRequest{{Length: 4}, {Length: 4}, {Length: 4}}

	results, err := conn.client.Load().parseSumReadResponse(resp, 3, requests)
	if err != nil {
		t.Fatalf("unexpected outer error: %v", err)
	}

	if results[0].Error != ReturnCodeNoErrors {
		t.Errorf("results[0].Error = %v, want NoErrors", results[0].Error)
	}
	if !bytes.Equal(results[0].Data, item1) {
		t.Errorf("results[0].Data = %v, want %v", results[0].Data, item1)
	}
	if results[1].Error != ReturnCodeDeviceSymbolVersionInvalid {
		t.Errorf("results[1].Error = %v, want SymbolVersionInvalid", results[1].Error)
	}
	if results[2].Error != ReturnCodeNoErrors {
		t.Errorf("results[2].Error = %v, want NoErrors (alignment preserved across errored item)", results[2].Error)
	}
	if !bytes.Equal(results[2].Data, item2) {
		t.Errorf("results[2].Data = %v, want %v — errored item must advance dataOffset", results[2].Data, item2)
	}
}

// TestParseSumReadResponse_ErroredItemOverflowsRemaining: an errored item
// declaring a length exceeding the bytes remaining in the response data
// section must cascade-mark every remaining item DeviceInvalidSize, mirroring
// the per-item-oversize / truncation path. Prevents silent garbage parsing.
func TestParseSumReadResponse_ErroredItemOverflowsRemaining(t *testing.T) {
	var logBuf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logBuf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	conn := &Session{logger: logger}
	conn.client.Store(&Client{logger: logger})

	// Two items, second errored with absurd declared length, only 4 actual data bytes.
	resp := craftSumReadResponse(
		[]ReturnCode{ReturnCodeNoErrors, ReturnCodeDeviceSymbolVersionInvalid},
		[]uint32{4, 0xFFFFFFFE}, // item 1 errored, claims ~4 GiB; remaining < that
		[]byte{1, 2, 3, 4},
	)
	requests := []SumReadRequest{{Length: 4}, {Length: 4}}

	results, err := conn.client.Load().parseSumReadResponse(resp, 2, requests)
	if err != nil {
		t.Fatalf("unexpected outer error: %v", err)
	}
	if results[1].Error != ReturnCodeDeviceInvalidSize {
		t.Errorf("results[1].Error = %v, want DeviceInvalidSize (errored-item overflow cascade)", results[1].Error)
	}
	if !strings.Contains(logBuf.String(), "errored-item declared length exceeds remaining bytes") {
		t.Errorf("expected errored-item-overflow log, got: %s", logBuf.String())
	}
}
