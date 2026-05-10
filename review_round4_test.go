package ads

import (
	"testing"
	"unicode/utf16"
)

// TestAddOffsetDepthCap verifies the recursion cap defends against malformed
// PLC datatype tables that form a self-cycle (forbidden by IEC 61131-3 but
// not enforced over the wire).
// Validates: R-SYM-002.
func TestAddOffsetDepthCap(t *testing.T) {
	// Build a self-cycle: type "MyStruct" has a child of type "MyStruct".
	// Real PLCs reject this at compile time; the wire response could in
	// theory contain it through a buggy or malicious target.
	cyclic := SymbolUploadDataType{
		DataType: "MyStruct",
		Children: map[string]*SymbolUploadDataType{
			"member": {
				Name:     "member",
				DataType: "MyStruct",
				DatatypeEntry: datatypeEntry{
					Size: 4,
					Offs: 0,
				},
			},
		},
	}
	datatypes := map[string]SymbolUploadDataType{
		"MyStruct": cyclic,
	}

	parent := &Symbol{Name: "root", FullName: "root", Length: 4}
	// Should NOT stack-overflow even though the cycle would otherwise recurse
	// indefinitely.
	children := cyclic.addOffset(parent, datatypes, 0x4020)
	if len(children) == 0 {
		t.Fatal("expected at least one child before depth cap fired")
	}
	// The cap fires partway down; depth 256 produces ~256 nested children
	// before the warn-and-return. Sanity check we didn't blow the stack.
}

// TestCollectSubtreeDepthCap verifies the recursion cap on Symbol.Children
// walks defends against tree corruption (cycle in Children map).
// Validates: R-VIEW-004.
func TestCollectSubtreeDepthCap(t *testing.T) {
	// Build a Symbol cycle: A -> B -> A. addOffset cannot produce this in
	// real cache data, but defensively we should not stack-overflow.
	a := &Symbol{Name: "A", FullName: "A"}
	b := &Symbol{Name: "B", FullName: "A.B"}
	a.Children = map[string]*Symbol{"B": b}
	b.Children = map[string]*Symbol{"A": a}

	conn := newTestConnection()
	defer conn.lifecycle.shutdown()

	// Walk via collectSubtree (lock not actually needed for this test
	// because we are not touching cache.symbols).
	var collected []SymbolView
	collectSubtree(a, conn, &collected)
	// Should NOT stack-overflow; cap=256 limits the walk.
	if len(collected) == 0 {
		t.Fatal("expected at least one collected view before cap fires")
	}
}

// TestWSTRINGSurrogatePairTruncation verifies that truncation of a WSTRING
// write that lands on a UTF-16 high surrogate drops the unpaired surrogate
// instead of writing a malformed sequence to the PLC.
// Validates: R-PARSE-004.
func TestWSTRINGSurrogatePairTruncation(t *testing.T) {
	// String with one BMP char followed by a non-BMP char (encodes as 2
	// UTF-16 code units forming a surrogate pair).
	// "A" = 1 code unit, "𝐀" (MATHEMATICAL BOLD CAPITAL A, U+1D400) = 2
	// code units (surrogate pair).
	value := "A\U0001D400"
	encoded := utf16.Encode([]rune(value))
	if len(encoded) != 3 {
		t.Fatalf("expected 3 UTF-16 code units, got %d", len(encoded))
	}
	if encoded[0] != 0x0041 {
		t.Errorf("expected 'A' = 0x0041, got 0x%04X", encoded[0])
	}
	if encoded[1] < 0xD800 || encoded[1] > 0xDBFF {
		t.Errorf("expected high surrogate in 0xD800-0xDBFF, got 0x%04X", encoded[1])
	}
	if encoded[2] < 0xDC00 || encoded[2] > 0xDFFF {
		t.Errorf("expected low surrogate in 0xDC00-0xDFFF, got 0x%04X", encoded[2])
	}

	// Symbol with Length such that maxChars = (Length-2)/2 lands the
	// truncation between the high+low surrogates. Length=6 => maxChars=2,
	// truncate keeps encoded[0:2] = ['A', high-surrogate]. The surrogate
	// fix should drop the high surrogate to avoid emitting an unpaired
	// surrogate to the PLC.
	sym := &Symbol{
		Name:     "ws",
		FullName: "ws",
		DataType: "WSTRING",
		Length:   6, // 2 chars + 2-byte null = 4+2
	}
	data, err := sym.writeToNode(value, nil)
	if err != nil {
		t.Fatalf("writeToNode error: %v", err)
	}
	if len(data) != 6 {
		t.Fatalf("expected 6 bytes (3 code units), got %d", len(data))
	}
	// First 2 bytes: 'A' little-endian = 0x41 0x00
	if data[0] != 0x41 || data[1] != 0x00 {
		t.Errorf("expected 'A' at bytes 0-1, got 0x%02X 0x%02X", data[0], data[1])
	}
	// Bytes 2-3 should be ZEROS (the would-be high surrogate dropped),
	// followed by 2-byte null terminator at bytes 4-5.
	if data[2] != 0x00 || data[3] != 0x00 {
		t.Errorf("expected high surrogate dropped (zero bytes 2-3 - either truncated or null), got 0x%02X 0x%02X", data[2], data[3])
	}
}

// TestSumNotificationResultTriState drives the production
// Session.AddSymbolNotifications path through the scriptable PLC stub
// and asserts the three+TOCTOU classification of the
// SumNotificationResult struct returned to the caller:
//
//  1. success — Handle != 0, Error == NoErrors, Skipped == nil.
//  2. PLC error — Handle == 0, Error != NoErrors, Skipped == nil.
//  3. library skip (duplicate name in batch) — Skipped != nil.
//  4. TOCTOU loss (PLC accepted, library found stranded *Symbol
//     post-roundtrip) — Skipped != nil, Handle may be non-zero so
//     caller must release.
//
// Validates: R-NOT-009 (per-config result contract) / R-SUM-004
// (sum-batch tri-state).
func TestSumNotificationResultTriState(t *testing.T) {
	srv := startScriptableServer(t)
	defer srv.stop()
	sess, _ := newWiredTestSession(t, srv)

	// Three symbols cached up-front: x, y, z.
	for _, name := range []string{"MAIN.x", "MAIN.y", "MAIN.z"} {
		sess.cache.symbols[symbolKey(name)] = &Symbol{
			FullName:    name,
			DataType:    "INT",
			Length:      2,
			Handle:      0xA1B2C3D4, // any non-zero handle so symbolSumAddress takes the handle path
			ContextMask: 0,
		}
	}

	// Sum-add response: per-item shape based on inbound count. Item 0
	// (x) succeeds with handle 0x1001. Item 1 (y) returns PLC error.
	// Item 2 (z) succeeds with handle 0x1003 — but the test mutates
	// the cache mid-handler so the post-roundtrip re-fetch finds the
	// orphan and reports Skipped+Handle (TOCTOU race).
	srv.onWriteRead(GroupSumupAddDeviceNotification, func(req []byte) []byte {
		// Mid-roundtrip: delete z from the cache so the post-roundtrip
		// re-resolve fails for that handle, triggering the TOCTOU branch.
		sess.cache.lock.Lock()
		delete(sess.cache.symbols, symbolKey("MAIN.z"))
		sess.cache.lock.Unlock()
		return buildSumAddNotifPayload([]sumNotifResponse{
			{Handle: 0x1001, Error: ReturnCodeNoErrors},
			{Handle: 0, Error: ReturnCodeDeviceInvalidParam},
			{Handle: 0x1003, Error: ReturnCodeNoErrors},
		})
	})
	// bestEffortDelete uses SumDelete for the orphan release.
	srv.onWriteRead(GroupSumupDeleteDeviceNotification, func(req []byte) []byte {
		nItems := len(req) / 4
		codes := make([]ReturnCode, nItems)
		for i := range codes {
			codes[i] = ReturnCodeNoErrors
		}
		return buildSumDeleteNotifPayload(codes)
	})

	ch := make(chan *Update, 4)
	configs := []NotificationConfig{
		{SymbolName: "MAIN.x", TransmissionMode: TransModeServerOnChange},
		// Library-skip case: duplicate name within the batch.
		{SymbolName: "MAIN.x", TransmissionMode: TransModeServerOnChange},
		{SymbolName: "MAIN.y", TransmissionMode: TransModeServerOnChange},
		{SymbolName: "MAIN.z", TransmissionMode: TransModeServerOnChange},
	}

	results, err := sess.AddSymbolNotifications(configs, ch)
	if err != nil {
		t.Fatalf("AddSymbolNotifications: %v", err)
	}
	if len(results) != len(configs) {
		t.Fatalf("got %d results, want %d", len(results), len(configs))
	}

	// Assert: configs[0] success
	r0 := results[0]
	if r0.Skipped != nil || r0.Error != ReturnCodeNoErrors || r0.Handle == 0 {
		t.Errorf("config[0] (success): got Handle=%d Error=%v Skipped=%v",
			r0.Handle, r0.Error, r0.Skipped)
	}
	// Assert: configs[1] library-skip duplicate (Skipped != nil)
	r1 := results[1]
	if r1.Skipped == nil {
		t.Errorf("config[1] (duplicate): Skipped should be non-nil; got %+v", r1)
	}
	// Assert: configs[2] PLC error (Skipped nil, Error != NoErrors, Handle == 0)
	r2 := results[2]
	if r2.Skipped != nil || r2.Error == ReturnCodeNoErrors || r2.Handle != 0 {
		t.Errorf("config[2] (PLC error): got Handle=%d Error=%v Skipped=%v",
			r2.Handle, r2.Error, r2.Skipped)
	}
	// Assert: configs[3] TOCTOU loss (Skipped != nil, Handle non-zero from
	// the PLC because the cache vanished mid-roundtrip — caller MUST
	// release this handle on the PLC side via DeleteDeviceNotification).
	r3 := results[3]
	if r3.Skipped == nil {
		t.Errorf("config[3] (TOCTOU): Skipped should be non-nil; got %+v", r3)
	}
}
