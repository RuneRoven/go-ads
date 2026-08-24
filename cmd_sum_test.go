package ads

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
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

	// The stale item now comes back as a per-item failure in a *BatchError
	// (DECISIONS.md Decision 1); detection must still fire regardless.
	values, err := sess.ReadMultipleSymbols(context.Background(), []string{"MAIN.a", "MAIN.b"})
	batchErr := batchErrorFor(t, err)
	if len(batchErr.Items) != 1 || batchErr.Items[0].Symbol != "MAIN.a" {
		t.Errorf("got failed items %v, want just MAIN.a", batchErr.Items)
	}
	if _, ok := values["MAIN.b"]; !ok {
		t.Errorf("got values = %v, want the readable symbol MAIN.b present", values)
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

	// All three items carry a failing code, so the call now reports an
	// all-failed batch (DECISIONS.md Decision 1) — asserted here rather than
	// ignored, so this test still notices if the read path stops reporting it.
	// The subject of the test is unchanged: detection fires exactly once.
	_, err := sess.ReadMultipleSymbols(context.Background(), []string{"MAIN.a", "MAIN.b", "MAIN.c"})
	batchErr := batchErrorFor(t, err)
	if len(batchErr.Items) != 3 || batchErr.Succeeded != 0 {
		t.Errorf("got Succeeded=%d failed items %v, want 0 succeeded and all three failed",
			batchErr.Succeeded, batchErr.Items)
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

	// The rejected item is now named in a *BatchError (DECISIONS.md Decision 1);
	// detection must still fire.
	codes, err := sess.WriteMultipleSymbols(context.Background(), map[string]string{
		"MAIN.a": "true",
		"MAIN.b": "false",
	})
	// Which name lands in slot 0 depends on Go's map iteration order, so assert
	// the shape rather than the identity: one rejected, one accepted.
	batchErr := batchErrorFor(t, err)
	if len(batchErr.Items) != 1 || batchErr.Succeeded != 1 {
		t.Errorf("got Succeeded=%d failed items %v, want 1 of each", batchErr.Succeeded, batchErr.Items)
	}
	if len(codes) != 2 {
		t.Errorf("got codes = %v, want a code for both symbols", codes)
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

// TestAMSRouterErrorIsNotADeviceVerdict pins the provenance invariant at the one
// line where it used to be lost: amsReply.payload() wraps the AMS header's
// ErrorCode, and the result must not look like something the PLC said about an
// item. Every abort guard in this package (cmd_sum.go's notification fallbacks
// among them) decides "transport failure" vs "device verdict" with
// errors.As(err, &ReturnCode), so a router code that satisfies errors.As is
// silently promoted to a per-item PLC verdict.
//
// The code itself must stay readable through errors.Is — client_test.go's AMS
// test and every consumer branching on a named router condition depend on that.
func TestAMSRouterErrorIsNotADeviceVerdict(t *testing.T) {
	_, err := amsReply{amsErr: ReturnCodeGlobalTargetPortNotFound}.payload()
	if err == nil {
		t.Fatal("payload() returned nil error for a non-zero AMS ErrorCode")
	}
	// Double wrap is the real shape: executeSumCommand and the notification
	// fallbacks each add their own %w on the way out.
	wrapped := fmt.Errorf("outer: %w", fmt.Errorf("inner: %w", err))

	for _, tc := range []struct {
		name string
		err  error
	}{
		{name: "bare", err: err},
		{name: "double wrapped", err: wrapped},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var rc ReturnCode
			if errors.As(tc.err, &rc) {
				t.Errorf("errors.As extracted ReturnCode %v from a router rejection; "+
					"every abort guard will read it as a PLC verdict about an item", rc)
			}
			if !errors.Is(tc.err, ReturnCodeGlobalTargetPortNotFound) {
				t.Errorf("errors.Is(err, ReturnCodeGlobalTargetPortNotFound) = false, want true; got %v", tc.err)
			}
			if errors.Is(tc.err, ReturnCodeGlobalInsertMailboxError) {
				t.Error("errors.Is matched an unrelated router code")
			}
			if !strings.Contains(tc.err.Error(), "target port") {
				t.Errorf("error text lost the code name: %q", tc.err.Error())
			}
			// A router rejection IS an answer from the far side. startRuntimeStateWatch
			// retires the system-service poll on a run of answers; if this reads false,
			// a device with no system service port never retires and the watcher polls
			// for the life of the session.
			if !isDeviceAnswer(tc.err) {
				t.Error("isDeviceAnswer = false for an AMS router rejection, want true")
			}
			// The reconnect loop's unserved cooldown must stay out of this: the
			// router answered, so this is not "accepted the connection then said
			// nothing".
			if isUnservedError(tc.err) {
				t.Error("isUnservedError = true for an AMS router rejection, want false")
			}
			// Only device codes may drive capability latching and stale-cache
			// detection. A router code reaching either poisons session-wide state.
			if isSumCommandUnsupportedError(tc.err) {
				t.Error("isSumCommandUnsupportedError = true for a router rejection, want false")
			}
		})
	}
}

// --- batch error contract (DECISIONS.md Decision 1) ---
//
// The contract these pin: a batch call returns the values it obtained plus a
// *BatchError naming every item that produced none. A bare error is reserved
// for a transport failure, where no item's outcome is known. Batch size does
// not change the shape of any of this.

// batchErrorFor extracts the *BatchError from err, failing the test if err is
// nil or is a different kind of error.
func batchErrorFor(t *testing.T, err error) *BatchError {
	t.Helper()
	if err == nil {
		t.Fatal("got err = nil, want a *BatchError naming the failed items")
	}
	var batchErr *BatchError
	if !errors.As(err, &batchErr) {
		t.Fatalf("got err = %v (%T), want a *BatchError", err, err)
	}
	return batchErr
}

// itemFor returns the BatchError entry for symbol, or fails the test.
func itemFor(t *testing.T, batchErr *BatchError, symbol string) BatchItemError {
	t.Helper()
	for _, item := range batchErr.Items {
		if item.Symbol == symbol {
			return item
		}
	}
	t.Fatalf("no BatchError item for %q; got %v", symbol, batchErr.Items)
	return BatchItemError{}
}

// TestReadMultipleSymbols_AllItemsFailedIsNotSuccess pins the measured TC3
// failure: after a runtime restart every cached handle is refused, so every
// item comes back 0x710. Before the batch error contract this returned an
// empty map with err == nil — total data loss reported as success.
//
// Validates: DECISIONS.md Decision 1 (per-item status, PLC-verdict state).
func TestReadMultipleSymbols_AllItemsFailedIsNotSuccess(t *testing.T) {
	srv := startScriptableServer(t)
	defer srv.stop()

	sess, _ := newWiredTestSession(t, srv, WithSymbolVersionStrategy(SymbolVersionIgnore))
	seedSymbol(sess, "MAIN.a", 0x4001)
	seedSymbol(sess, "MAIN.b", 0x4002)
	seedSymbol(sess, "MAIN.c", 0x4003)

	srv.onWriteRead(GroupSumupReadEx2, func(_ []byte) []byte {
		return craftSumReadResponse(
			[]ReturnCode{
				ReturnCodeDeviceSymbolNoFound,
				ReturnCodeDeviceSymbolNoFound,
				ReturnCodeDeviceSymbolNoFound,
			},
			[]uint32{0, 0, 0},
			nil,
		)
	})

	names := []string{"MAIN.a", "MAIN.b", "MAIN.c"}
	values, err := sess.ReadMultipleSymbols(context.Background(), names)
	batchErr := batchErrorFor(t, err)

	if len(values) != 0 {
		t.Errorf("got %d values for a batch where every item was refused, want 0: %v", len(values), values)
	}
	if batchErr.Requested != 3 || batchErr.Succeeded != 0 || len(batchErr.Items) != 3 {
		t.Errorf("got Requested=%d Succeeded=%d items=%d, want 3/0/3",
			batchErr.Requested, batchErr.Succeeded, len(batchErr.Items))
	}
	for _, item := range batchErr.Items {
		// A refused handle is a device verdict, not a library-side skip: the
		// caller must be able to tell those apart to know whether retrying
		// the same request could ever work.
		if item.Skipped != nil {
			t.Errorf("item %s: Skipped = %v, want nil (the PLC gave a verdict)", item.Symbol, item.Skipped)
		}
		if item.Error != ReturnCodeDeviceSymbolNoFound {
			t.Errorf("item %s: Error = 0x%X, want 0x%X", item.Symbol, uint32(item.Error), uint32(ReturnCodeDeviceSymbolNoFound))
		}
	}
}

// TestReadMultipleSymbols_OneAbsentSymbolKeepsTheRest pins the constraint that
// one misspelled tag must not stop the other values flowing: the successful
// items stay in the map and only the failed one is named.
//
// Validates: DECISIONS.md Decision 1 (partial success stays usable).
func TestReadMultipleSymbols_OneAbsentSymbolKeepsTheRest(t *testing.T) {
	srv := startScriptableServer(t)
	defer srv.stop()

	sess, _ := newWiredTestSession(t, srv, WithSymbolVersionStrategy(SymbolVersionIgnore))
	seedSymbol(sess, "MAIN.a", 0x4101)
	seedSymbol(sess, "MAIN.absent", 0x4102)
	seedSymbol(sess, "MAIN.c", 0x4103)

	srv.onWriteRead(GroupSumupReadEx2, func(_ []byte) []byte {
		return craftSumReadResponse(
			[]ReturnCode{ReturnCodeNoErrors, ReturnCodeDeviceSymbolNoFound, ReturnCodeNoErrors},
			[]uint32{1, 0, 1},
			[]byte{0x01, 0x00},
		)
	})

	names := []string{"MAIN.a", "MAIN.absent", "MAIN.c"}
	values, err := sess.ReadMultipleSymbols(context.Background(), names)
	batchErr := batchErrorFor(t, err)

	if len(values) != 2 || values["MAIN.a"] == "" || values["MAIN.c"] == "" {
		t.Errorf("got values = %v, want the two readable symbols present", values)
	}
	if len(batchErr.Items) != 1 {
		t.Fatalf("got %d failed items, want 1: %v", len(batchErr.Items), batchErr.Items)
	}
	if batchErr.Succeeded != 2 {
		t.Errorf("got Succeeded = %d, want 2", batchErr.Succeeded)
	}
	item := itemFor(t, batchErr, "MAIN.absent")
	if item.Skipped != nil || item.Error != ReturnCodeDeviceSymbolNoFound {
		t.Errorf("got item %+v, want a PLC verdict of 0x710", item)
	}
	if !strings.Contains(err.Error(), "MAIN.absent") {
		t.Errorf("error message %q does not name the failed symbol", err.Error())
	}
}

// TestReadMultipleSymbols_UnresolvedSymbolIsReported covers the drop site where
// getSymbol fails, so the item never reaches the wire. Previously the name was
// dropped from the result map with no error whenever any other item decoded.
//
// Validates: DECISIONS.md Decision 1 (Skipped state, resolve-time drop).
func TestReadMultipleSymbols_UnresolvedSymbolIsReported(t *testing.T) {
	srv := startScriptableServer(t)
	defer srv.stop()

	sess, _ := newWiredTestSession(t, srv, WithSymbolVersionStrategy(SymbolVersionIgnore))
	seedSymbol(sess, "MAIN.a", 0x4201)
	// MAIN.typo is not seeded, and the server answers no handle lookup, so
	// getSymbol fails for it before any request is built.

	srv.onWriteRead(GroupSumupReadEx2, func(_ []byte) []byte {
		return craftSumReadResponse([]ReturnCode{ReturnCodeNoErrors}, []uint32{1}, []byte{0x01})
	})

	values, err := sess.ReadMultipleSymbols(context.Background(), []string{"MAIN.a", "MAIN.typo"})
	batchErr := batchErrorFor(t, err)

	if len(values) != 1 || values["MAIN.a"] == "" {
		t.Errorf("got values = %v, want MAIN.a present", values)
	}
	item := itemFor(t, batchErr, "MAIN.typo")
	if !errors.Is(item.Skipped, ErrBatchSymbolUnresolved) {
		t.Errorf("got Skipped = %v, want it to match ErrBatchSymbolUnresolved", item.Skipped)
	}
	if !errors.Is(err, ErrBatchSymbolUnresolved) {
		t.Error("errors.Is(err, ErrBatchSymbolUnresolved) = false; the skip reason must be reachable from the returned error")
	}

	// Nothing resolvable at all takes an earlier return, and it must produce the
	// same shape — the contract cannot depend on how many items survived.
	values, err = sess.ReadMultipleSymbols(context.Background(), []string{"MAIN.typo", "MAIN.other"})
	batchErr = batchErrorFor(t, err)
	if len(values) != 0 {
		t.Errorf("got values = %v, want none", values)
	}
	if batchErr.Requested != 2 || len(batchErr.Items) != 2 {
		t.Errorf("got Requested=%d items=%v, want both names reported", batchErr.Requested, batchErr.Items)
	}
}

// TestReadMultipleSymbols_VanishedAndUnparsableAreReported covers the two
// post-roundtrip drop sites: the cache entry disappearing mid-roundtrip
// (live == nil) and the payload failing to decode against the cached type,
// which is what an undetected INT→LREAL online change looks like. Both used to
// leave the name missing from the map with err == nil, permanently.
//
// Validates: DECISIONS.md Decision 1 (Skipped state, decode-time drops).
func TestReadMultipleSymbols_VanishedAndUnparsableAreReported(t *testing.T) {
	srv := startScriptableServer(t)
	defer srv.stop()

	sess, _ := newWiredTestSession(t, srv, WithSymbolVersionStrategy(SymbolVersionIgnore))
	seedSymbol(sess, "MAIN.a", 0x4301)
	seedSymbol(sess, "MAIN.gone", 0x4302)
	seedSymbol(sess, "MAIN.widened", 0x4303)

	// The handler runs after the requests are on the wire and before the decode
	// loop takes cache.lock, so it can stage exactly the two races: drop one
	// entry from the cache, and widen another's Length past the payload the PLC
	// is about to return.
	srv.onWriteRead(GroupSumupReadEx2, func(_ []byte) []byte {
		sess.cache.lock.Lock()
		delete(sess.cache.symbols, symbolKey("MAIN.gone"))
		sess.cache.symbols[symbolKey("MAIN.widened")].Length = 8
		sess.cache.lock.Unlock()
		return craftSumReadResponse(
			[]ReturnCode{ReturnCodeNoErrors, ReturnCodeNoErrors, ReturnCodeNoErrors},
			[]uint32{1, 1, 1},
			[]byte{0x01, 0x01, 0x01},
		)
	})

	names := []string{"MAIN.a", "MAIN.gone", "MAIN.widened"}
	values, err := sess.ReadMultipleSymbols(context.Background(), names)
	batchErr := batchErrorFor(t, err)

	if len(values) != 1 || values["MAIN.a"] == "" {
		t.Errorf("got values = %v, want only MAIN.a", values)
	}
	if got := itemFor(t, batchErr, "MAIN.gone"); !errors.Is(got.Skipped, ErrBatchSymbolVanished) {
		t.Errorf("MAIN.gone: Skipped = %v, want ErrBatchSymbolVanished", got.Skipped)
	}
	if got := itemFor(t, batchErr, "MAIN.widened"); !errors.Is(got.Skipped, ErrBatchValueUnparsable) {
		t.Errorf("MAIN.widened: Skipped = %v, want ErrBatchValueUnparsable", got.Skipped)
	}
}

// TestReadMultipleSymbols_TransportFailureIsABareError pins the other half of
// the contract: a router-level rejection is not a per-item verdict, so it must
// NOT arrive as a *BatchError. A caller that unwraps one and reads the map
// would be trusting values that were never fetched.
//
// Validates: DECISIONS.md Decision 1 (bare error reserved for transport).
func TestReadMultipleSymbols_TransportFailureIsABareError(t *testing.T) {
	srv := startScriptableServer(t)
	defer srv.stop()

	sess, _ := newWiredTestSession(t, srv, WithSymbolVersionStrategy(SymbolVersionIgnore))
	seedSymbol(sess, "MAIN.a", 0x4401)
	seedSymbol(sess, "MAIN.b", 0x4402)

	// Seeded symbols need no handle lookup, so the first ReadWrite is the
	// SumRead itself: the router rejects it the way a PLC in CONFIG does.
	srv.amsErrorAfter(CommandIDReadWrite, 1, ReturnCodeGlobalTargetPortNotFound)

	values, err := sess.ReadMultipleSymbols(context.Background(), []string{"MAIN.a", "MAIN.b"})
	if err == nil {
		t.Fatal("got err = nil for a router-rejected batch")
	}
	var batchErr *BatchError
	if errors.As(err, &batchErr) {
		t.Errorf("got a *BatchError (%v) for a transport failure; per-item results are not knowable here", batchErr)
	}
	if !errors.Is(err, ReturnCodeGlobalTargetPortNotFound) {
		t.Errorf("got err = %v, want it to match ReturnCodeGlobalTargetPortNotFound", err)
	}
	if values != nil {
		t.Errorf("got values = %v, want nil on transport failure", values)
	}
}

// TestReadMultipleSymbols_EmptyRequestIsNotAnError guards the boundary the
// contract keeps unchanged: asking for nothing is not a failure.
//
// Validates: DECISIONS.md Decision 1 (empty request stays nil, nil).
func TestReadMultipleSymbols_EmptyRequestIsNotAnError(t *testing.T) {
	srv := startScriptableServer(t)
	defer srv.stop()

	sess, _ := newWiredTestSession(t, srv, WithSymbolVersionStrategy(SymbolVersionIgnore))

	for _, names := range [][]string{nil, {}} {
		values, err := sess.ReadMultipleSymbols(context.Background(), names)
		if err != nil || values != nil {
			t.Errorf("ReadMultipleSymbols(%v) = %v, %v; want nil, nil", names, values, err)
		}
	}
}

// TestWriteMultipleSymbols_DroppedItemIsNotSuccess pins the write half. A
// dropped item is absent from the returned map, and ReturnCodeNoErrors is 0, so
// the idiomatic per-symbol check reads a write that never happened as a write
// that succeeded. Only the error can distinguish them.
//
// Validates: DECISIONS.md Decision 1 (write-path dropped item).
func TestWriteMultipleSymbols_DroppedItemIsNotSuccess(t *testing.T) {
	srv := startScriptableServer(t)
	defer srv.stop()

	sess, _ := newWiredTestSession(t, srv, WithSymbolVersionStrategy(SymbolVersionIgnore))
	seedSymbol(sess, "MAIN.a", 0x4501)
	seedSymbol(sess, "MAIN.bad", 0x4502)
	// MAIN.typo is not seeded: getSymbol fails and the setpoint never reaches
	// the PLC. MAIN.bad is a BOOL handed a non-boolean, the shape a type change
	// under an online change takes, and it dies at serialization instead.

	srv.onWriteRead(GroupSumupWrite, func(_ []byte) []byte {
		buf := make([]byte, 4)
		binary.LittleEndian.PutUint32(buf, uint32(ReturnCodeNoErrors))
		return buf
	})

	codes, err := sess.WriteMultipleSymbols(context.Background(), map[string]string{
		"MAIN.a":    "true",
		"MAIN.bad":  "not-a-bool",
		"MAIN.typo": "true",
	})
	batchErr := batchErrorFor(t, err)

	if codes["MAIN.a"] != ReturnCodeNoErrors {
		t.Errorf("MAIN.a: code = 0x%X, want 0x0 — the write that did land must still report success", uint32(codes["MAIN.a"]))
	}
	if _, ok := codes["MAIN.typo"]; ok {
		t.Error("MAIN.typo is present in the code map; a write that never happened must not carry a code")
	}
	item := itemFor(t, batchErr, "MAIN.typo")
	if !errors.Is(item.Skipped, ErrBatchSymbolUnresolved) {
		t.Errorf("MAIN.typo: Skipped = %v, want ErrBatchSymbolUnresolved", item.Skipped)
	}
	if bad := itemFor(t, batchErr, "MAIN.bad"); !errors.Is(bad.Skipped, ErrBatchValueUnserializable) {
		t.Errorf("MAIN.bad: Skipped = %v, want ErrBatchValueUnserializable", bad.Skipped)
	}
	if batchErr.Requested != 3 || batchErr.Succeeded != 1 {
		t.Errorf("got Requested=%d Succeeded=%d, want 3/1", batchErr.Requested, batchErr.Succeeded)
	}
}

// TestWriteMultipleSymbols_PerItemRejectionIsAnError covers the write path's
// device-verdict state: the batch reached the PLC and one item was rejected.
//
// Validates: DECISIONS.md Decision 1 (write-path PLC verdict).
func TestWriteMultipleSymbols_PerItemRejectionIsAnError(t *testing.T) {
	srv := startScriptableServer(t)
	defer srv.stop()

	sess, _ := newWiredTestSession(t, srv, WithSymbolVersionStrategy(SymbolVersionIgnore))
	seedSymbol(sess, "MAIN.a", 0x4601)

	srv.onWriteRead(GroupSumupWrite, func(_ []byte) []byte {
		buf := make([]byte, 4)
		binary.LittleEndian.PutUint32(buf, uint32(ReturnCodeDeviceSymbolNoFound))
		return buf
	})

	codes, err := sess.WriteMultipleSymbols(context.Background(), map[string]string{"MAIN.a": "true"})
	batchErr := batchErrorFor(t, err)

	if codes["MAIN.a"] != ReturnCodeDeviceSymbolNoFound {
		t.Errorf("MAIN.a: code = 0x%X, want 0x710", uint32(codes["MAIN.a"]))
	}
	item := itemFor(t, batchErr, "MAIN.a")
	if item.Skipped != nil || item.Error != ReturnCodeDeviceSymbolNoFound {
		t.Errorf("got item %+v, want a PLC verdict of 0x710", item)
	}
}

// TestBatchSymbols_FullSuccessIsNotAnError guards the other end of the
// contract: when every item succeeds there is no error at all. Without this,
// nothing stops the batch error from firing on a healthy batch — every caller
// would see a permanent failure.
//
// Validates: DECISIONS.md Decision 1 (error only when an item failed).
func TestBatchSymbols_FullSuccessIsNotAnError(t *testing.T) {
	srv := startScriptableServer(t)
	defer srv.stop()

	sess, _ := newWiredTestSession(t, srv, WithSymbolVersionStrategy(SymbolVersionIgnore))
	seedSymbol(sess, "MAIN.a", 0x4701)
	seedSymbol(sess, "MAIN.b", 0x4702)

	srv.onWriteRead(GroupSumupReadEx2, func(_ []byte) []byte {
		return craftSumReadResponse(
			[]ReturnCode{ReturnCodeNoErrors, ReturnCodeNoErrors},
			[]uint32{1, 1},
			[]byte{0x01, 0x00},
		)
	})
	srv.onWriteRead(GroupSumupWrite, func(_ []byte) []byte {
		return make([]byte, 8) // two items, both ReturnCodeNoErrors
	})

	values, err := sess.ReadMultipleSymbols(context.Background(), []string{"MAIN.a", "MAIN.b"})
	if err != nil {
		t.Errorf("ReadMultipleSymbols on a healthy batch: got err = %v, want nil", err)
	}
	if len(values) != 2 {
		t.Errorf("got values = %v, want both symbols", values)
	}

	codes, err := sess.WriteMultipleSymbols(context.Background(), map[string]string{
		"MAIN.a": "true",
		"MAIN.b": "false",
	})
	if err != nil {
		t.Errorf("WriteMultipleSymbols on a healthy batch: got err = %v, want nil", err)
	}
	if len(codes) != 2 {
		t.Errorf("got codes = %v, want both symbols", codes)
	}
}
