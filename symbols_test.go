package ads

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"strconv"
	"strings"
	"testing"
)

// F-15: PLC-controlled Elements with no sanity cap allows DoS via huge map allocation.
// Cap at 1M entries per level. Reject and return empty map.
//
// Validates: R-SYM-002.
func TestMakeArrayChildren_CapsExcessiveElements(t *testing.T) {
	levels := []datatypeArrayInfo{{LBound: 0, Elements: 10_000_000}} // 10M
	got := makeArrayChildren(levels, "INT", 20_000_000)
	if got == nil {
		t.Fatalf("expected non-nil empty map on cap-exceeded, got nil")
	}
	if len(got) != 0 {
		t.Errorf("expected zero children when Elements exceeds cap, got %d", len(got))
	}
}

// F-15: LBound + Elements that overflows uint32 must be rejected.
//
// Validates: R-SYM-002.
func TestMakeArrayChildren_RejectsOverflowBound(t *testing.T) {
	levels := []datatypeArrayInfo{{LBound: 0xFFFFFFF0, Elements: 0x20}} // overflows
	got := makeArrayChildren(levels, "INT", 64)
	if got == nil {
		t.Fatalf("expected non-nil empty map on overflow, got nil")
	}
	if len(got) != 0 {
		t.Errorf("expected zero children on uint32 overflow, got %d", len(got))
	}
}

// Happy path: small array of 4 elements still works after the cap is added.
//
// Validates: NO-SPEC (regression guard, awaiting spec backfill).
// Pins makeArrayChildren happy-path output keys ("[0]".."[3]") for a 4-element array.
func TestMakeArrayChildren_HappyPath(t *testing.T) {
	levels := []datatypeArrayInfo{{LBound: 0, Elements: 4}}
	got := makeArrayChildren(levels, "INT", 8) // 8 bytes / 4 = 2 bytes each
	if len(got) != 4 {
		t.Errorf("expected 4 children, got %d", len(got))
	}
	for i := 0; i < 4; i++ {
		key := fmt.Sprintf("[%d]", i)
		if _, ok := got[key]; !ok {
			t.Errorf("missing child %q", key)
		}
	}
}

// --- parseUploadSymbolInfoDataTypes ---

// Validates: NO-SPEC (regression guard, awaiting spec backfill).
// Pins empty-buffer parse → empty map, no error.
func TestParseUploadSymbolInfoDataTypes_Empty(t *testing.T) {
	datatypes, err := parseUploadSymbolInfoDataTypes([]byte{})
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if len(datatypes) != 0 {
		t.Errorf("expected empty map, got %d entries", len(datatypes))
	}
}

// --- parseUploadSymbolInfoSymbols ---

// Validates: NO-SPEC (regression guard, awaiting spec backfill).
// Pins empty-buffer symbol-list parse → empty map, no error.
func TestParseUploadSymbolInfoSymbols_Empty(t *testing.T) {
	symbols, err := parseUploadSymbolInfoSymbols([]byte{}, nil)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if len(symbols) != 0 {
		t.Errorf("expected empty map, got %d entries", len(symbols))
	}
}

// --- addOffset / addChildren ---

// Validates: R-SYM-007.
func TestAddOffsetEmptySegmentName(t *testing.T) {
	parent := &Symbol{Name: "parent", FullName: "MAIN.parent"}
	dt := &SymbolUploadDataType{
		Children: map[string]*SymbolUploadDataType{
			"": {Name: "", DataType: "INT", DatatypeEntry: datatypeEntry{Size: 2}},
		},
	}
	children := dt.addOffset(parent, nil, 0)
	// Empty segment name should be skipped (F-06 fix)
	if len(children) != 0 {
		t.Errorf("expected 0 children for empty segment name, got %d", len(children))
	}
}

// Validates: R-SYM-007.
func TestAddOffsetFullNameWithDot(t *testing.T) {
	parent := &Symbol{Name: "motor", FullName: "MAIN.motor"}
	dt := &SymbolUploadDataType{
		Children: map[string]*SymbolUploadDataType{
			"speed": {Name: "speed", DataType: "INT", DatatypeEntry: datatypeEntry{Size: 2, Offs: 0}},
		},
	}
	children := dt.addOffset(parent, nil, 0)
	child, ok := children["speed"]
	if !ok {
		t.Fatal("expected child 'speed'")
	}
	if child.FullName != "MAIN.motor.speed" {
		t.Errorf("FullName = %q, want %q", child.FullName, "MAIN.motor.speed")
	}
}

// Validates: R-SYM-007.
func TestAddOffsetArrayFullName(t *testing.T) {
	// Array children have names like "[0]", "[1]" — should use parent.FullName (F-05 fix)
	parent := &Symbol{Name: "arr", FullName: "MAIN.arr"}
	dt := &SymbolUploadDataType{
		Children: map[string]*SymbolUploadDataType{
			"[0]": {Name: "[0]", DataType: "INT", DatatypeEntry: datatypeEntry{Size: 2, Offs: 0}},
			"[1]": {Name: "[1]", DataType: "INT", DatatypeEntry: datatypeEntry{Size: 2, Offs: 2}},
		},
	}
	children := dt.addOffset(parent, nil, 0)
	child0, ok := children["[0]"]
	if !ok {
		t.Fatal("expected child '[0]'")
	}
	// Should be "MAIN.arr[0]", NOT "arr[0]"
	if child0.FullName != "MAIN.arr[0]" {
		t.Errorf("FullName = %q, want %q", child0.FullName, "MAIN.arr[0]")
	}
}

// Validates: R-SYM-008.
func TestParseEnumNestedInStruct(t *testing.T) {
	// Non-strict enum: TwinCAT includes enum constants as sub-items.
	// Without the isEnumDataType guard, addOffset would expand these
	// constants as struct children, breaking parse.
	t.Run("non-strict enum with children", func(t *testing.T) {
		datatypes := map[string]SymbolUploadDataType{
			"E_MotorState": {
				Name:     "E_MotorState",
				DataType: "DINT",
				DatatypeEntry: datatypeEntry{
					Size:     4,
					SubItems: 3, // enum has 3 constants
				},
				Children: map[string]*SymbolUploadDataType{
					"Idle":    {Name: "Idle", DataType: "DINT", DatatypeEntry: datatypeEntry{Size: 4}},
					"Running": {Name: "Running", DataType: "DINT", DatatypeEntry: datatypeEntry{Size: 4}},
					"Error":   {Name: "Error", DataType: "DINT", DatatypeEntry: datatypeEntry{Size: 4}},
				},
			},
			"ST_Motor": {
				Name:     "ST_Motor",
				DataType: "",
				DatatypeEntry: datatypeEntry{
					Size:     8,
					SubItems: 2,
				},
				Children: map[string]*SymbolUploadDataType{
					"state": {Name: "state", DataType: "E_MotorState", DatatypeEntry: datatypeEntry{Size: 4, Offs: 0}},
					"speed": {Name: "speed", DataType: "INT", DatatypeEntry: datatypeEntry{Size: 2, Offs: 4}},
				},
			},
		}

		motorSym := &Symbol{
			Name: "motor", FullName: "MAIN.motor",
			DataType: "ST_Motor", Length: 8, Group: 0x4040,
		}
		dt := datatypes["ST_Motor"]
		motorSym.Children = dt.addOffset(motorSym, datatypes, motorSym.Group)

		// Wire data: state=2 (Running, DINT), speed=1500 (INT)
		data := make([]byte, 8)
		binary.LittleEndian.PutUint32(data[0:4], 2)
		binary.LittleEndian.PutUint16(data[4:6], 1500)

		value, err := motorSym.parse(data, 0, datatypes)
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
		t.Logf("parsed value: %s", value)

		stateChild := motorSym.Children["state"]
		if stateChild == nil {
			t.Fatal("expected child 'state'")
		}
		if stateChild.Value != "2" {
			t.Errorf("state value = %q, want %q", stateChild.Value, "2")
		}
		// Enum child must NOT have children (enum constants must not be expanded)
		if len(stateChild.Children) != 0 {
			t.Errorf("enum child should have 0 children, got %d", len(stateChild.Children))
		}

		speedChild := motorSym.Children["speed"]
		if speedChild == nil {
			t.Fatal("expected child 'speed'")
		}
		if speedChild.Value != "1500" {
			t.Errorf("speed value = %q, want %q", speedChild.Value, "1500")
		}
	})

	// Strict enum (TC3 with {attribute 'strict'}): no sub-items in datatype,
	// just a base type. This is what TC3 actually reports for qualified enums.
	t.Run("strict enum no children", func(t *testing.T) {
		datatypes := map[string]SymbolUploadDataType{
			"E_MachineState": {
				Name:     "E_MachineState",
				DataType: "DINT",
				DatatypeEntry: datatypeEntry{
					Size:     4,
					SubItems: 0, // strict: no enum constants exposed
				},
			},
			"ST_Motor": {
				Name:     "ST_Motor",
				DataType: "",
				DatatypeEntry: datatypeEntry{
					Size:     8,
					SubItems: 2,
				},
				Children: map[string]*SymbolUploadDataType{
					"state": {Name: "state", DataType: "E_MachineState", DatatypeEntry: datatypeEntry{Size: 4, Offs: 0}},
					"speed": {Name: "speed", DataType: "INT", DatatypeEntry: datatypeEntry{Size: 2, Offs: 4}},
				},
			},
		}

		motorSym := &Symbol{
			Name: "motor", FullName: "MAIN.motor",
			DataType: "ST_Motor", Length: 8, Group: 0x4040,
		}
		dt := datatypes["ST_Motor"]
		motorSym.Children = dt.addOffset(motorSym, datatypes, motorSym.Group)

		data := make([]byte, 8)
		binary.LittleEndian.PutUint32(data[0:4], 4) // ERROR=4
		binary.LittleEndian.PutUint16(data[4:6], 750)

		value, err := motorSym.parse(data, 0, datatypes)
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
		t.Logf("parsed value: %s", value)

		stateChild := motorSym.Children["state"]
		if stateChild == nil {
			t.Fatal("expected child 'state'")
		}
		if stateChild.Value != "4" {
			t.Errorf("state value = %q, want %q", stateChild.Value, "4")
		}
	})
}

// Validates: R-SYM-008.
func TestParseEnumWithoutDatatypes(t *testing.T) {
	// When datatypes table is nil (on-demand mode), enum types can be parsed
	// by inferring the base type from the symbol's byte size — but only for
	// 1- and 2-byte widths. REAL/LREAL share 4 and 8 byte widths with
	// DINT/LINT so the byte layout is ambiguous; inferBaseType refuses those
	// widths and the parser surfaces "unknown format" so the caller resolves
	// the type via LoadSymbols.
	successCases := []struct {
		name     string
		dataType string
		size     uint32
		data     []byte
		want     string
	}{
		{"1-byte enum", "E_SmallState", 1, []byte{42}, "42"},
		{"2-byte enum", "E_WordState", 2, func() []byte { b := make([]byte, 2); binary.LittleEndian.PutUint16(b, 1500); return b }(), "1500"},
	}
	for _, tt := range successCases {
		t.Run(tt.name, func(t *testing.T) {
			sym := &Symbol{
				Name:     "testEnum",
				FullName: "MAIN.testEnum",
				DataType: tt.dataType,
				Length:   tt.size,
			}
			// Pass nil datatypes — simulates on-demand mode
			value, err := sym.parse(tt.data, 0, nil)
			if err != nil {
				t.Fatalf("parse error: %v", err)
			}
			if value != tt.want {
				t.Errorf("got %q, want %q", value, tt.want)
			}
		})
	}

	// 4- and 8-byte unknown types must NOT be inferred — they could be a
	// REAL/LREAL alias just as plausibly as a DINT/LINT enum. Parser must
	// surface "unknown format" to push the caller to LoadSymbols.
	refusedCases := []struct {
		name     string
		dataType string
		size     uint32
	}{
		{"4-byte unknown refused (REAL/DINT ambiguity)", "E_MachineState", 4},
		{"8-byte unknown refused (LREAL/LINT ambiguity)", "E_BigState", 8},
	}
	for _, tt := range refusedCases {
		t.Run(tt.name, func(t *testing.T) {
			sym := &Symbol{
				Name:     "testEnum",
				FullName: "MAIN.testEnum",
				DataType: tt.dataType,
				Length:   tt.size,
			}
			_, err := sym.parse(make([]byte, tt.size), 0, nil)
			if err == nil {
				t.Fatalf("expected parse to refuse %d-byte inference, got nil", tt.size)
			}
		})
	}

	// Verify non-standard sizes still error
	t.Run("3-byte unknown type still errors", func(t *testing.T) {
		sym := &Symbol{
			Name: "weird", FullName: "MAIN.weird",
			DataType: "UNKNOWN_TYPE", Length: 3,
		}
		_, err := sym.parse([]byte{1, 2, 3}, 0, nil)
		if err == nil {
			t.Fatal("expected error for 3-byte unknown type")
		}
	})
}

// Validates: R-SYM-008.
func TestArrayTypedefNotMistakenForEnum(t *testing.T) {
	// A typedef array like "TYPE MyInts : ARRAY[0..2] OF INT;" has
	// Children (array elements) and DataType="INT" — same as an enum.
	// ArrayDim distinguishes them: arrays have ArrayDim > 0.
	datatypes := map[string]SymbolUploadDataType{
		"MyInts": {
			Name:     "MyInts",
			DataType: "INT",
			DatatypeEntry: datatypeEntry{
				Size:     6, // 3 x INT(2)
				ArrayDim: 1,
			},
			Children: map[string]*SymbolUploadDataType{
				"[0]": {Name: "[0]", DataType: "INT", DatatypeEntry: datatypeEntry{Size: 2, Offs: 0}},
				"[1]": {Name: "[1]", DataType: "INT", DatatypeEntry: datatypeEntry{Size: 2, Offs: 2}},
				"[2]": {Name: "[2]", DataType: "INT", DatatypeEntry: datatypeEntry{Size: 2, Offs: 4}},
			},
		},
		"ST_WithArray": {
			Name:     "ST_WithArray",
			DataType: "",
			DatatypeEntry: datatypeEntry{
				Size:     8,
				SubItems: 2,
			},
			Children: map[string]*SymbolUploadDataType{
				"values": {Name: "values", DataType: "MyInts", DatatypeEntry: datatypeEntry{Size: 6, Offs: 0}},
				"count":  {Name: "count", DataType: "INT", DatatypeEntry: datatypeEntry{Size: 2, Offs: 6}},
			},
		},
	}

	parent := &Symbol{
		Name:     "s",
		FullName: "MAIN.s",
		DataType: "ST_WithArray",
		Length:   8,
		Group:    0x4040,
		Offset:   0,
	}
	dt := datatypes["ST_WithArray"]
	parent.Children = dt.addOffset(parent, datatypes, parent.Group)

	// "values" child must have array element children expanded
	valuesChild, ok := parent.Children["values"]
	if !ok {
		t.Fatal("expected child 'values'")
	}
	if len(valuesChild.Children) != 3 {
		t.Fatalf("expected 3 array children for 'values', got %d", len(valuesChild.Children))
	}

	// Parse: values=[10, 20, 30], count=3
	data := make([]byte, 8)
	binary.LittleEndian.PutUint16(data[0:2], 10)
	binary.LittleEndian.PutUint16(data[2:4], 20)
	binary.LittleEndian.PutUint16(data[4:6], 30)
	binary.LittleEndian.PutUint16(data[6:8], 3)

	value, err := parent.parse(data, 0, datatypes)
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}
	t.Logf("parsed: %s", value)

	countChild, ok := parent.Children["count"]
	if !ok {
		t.Fatal("expected child 'count'")
	}
	if countChild.Value != "3" {
		t.Errorf("count = %q, want %q", countChild.Value, "3")
	}
}

// Validates: NO-SPEC (regression guard, awaiting spec backfill).
// Pins addChildren utility — child Symbol added to parent map under FullName key.
func TestAddChildren(t *testing.T) {
	child := &Symbol{Name: "x", FullName: "s.x", DataType: "INT", Length: 2}
	parent := &Symbol{
		Name: "s", FullName: "s", DataType: "ST_S", Length: 2,
		Children: map[string]*Symbol{"x": child},
	}
	symbols := map[string]*Symbol{"s": parent}
	addChildren(parent, symbols)
	if _, ok := symbols["s.x"]; !ok {
		t.Error("expected child 's.x' to be added to symbols map")
	}
}

// Validates: NO-SPEC (regression guard, awaiting spec backfill).
// Pins addChildren no-clobber: pre-existing entry under same key is preserved.
func TestAddChildrenNoDuplicates(t *testing.T) {
	child := &Symbol{Name: "x", FullName: "s.x", DataType: "INT", Length: 2}
	parent := &Symbol{
		Name: "s", FullName: "s", DataType: "ST_S", Length: 2,
		Children: map[string]*Symbol{"x": child},
	}
	// Pre-populate to ensure it doesn't overwrite
	existing := &Symbol{Name: "x", FullName: "s.x", DataType: "DINT", Length: 4}
	symbols := map[string]*Symbol{"s": parent, "s.x": existing}
	addChildren(parent, symbols)
	if symbols["s.x"].DataType != "DINT" {
		t.Error("addChildren should not overwrite existing symbols")
	}
}

// --- makeArrayChildren ---

// Validates: NO-SPEC (regression guard, awaiting spec backfill).
// Pins 1-D 3-element array child generation (keys "[0]".."[2]").
func TestMakeArrayChildren(t *testing.T) {
	levels := []datatypeArrayInfo{{LBound: 0, Elements: 3}}
	children := makeArrayChildren(levels, "INT", 6)
	if len(children) != 3 {
		t.Fatalf("expected 3 children, got %d", len(children))
	}
	for i := 0; i < 3; i++ {
		name := "[" + strconv.Itoa(i) + "]"
		child, ok := children[name]
		if !ok {
			t.Errorf("missing child %s", name)
			continue
		}
		if child.DataType != "INT" {
			t.Errorf("child %s datatype = %q, want %q", name, child.DataType, "INT")
		}
		if child.DatatypeEntry.Size != 2 {
			t.Errorf("child %s size = %d, want 2", name, child.DatatypeEntry.Size)
		}
	}
}

// Validates: NO-SPEC (regression guard, awaiting spec backfill).
// Pins makeArrayChildren on nil levels → empty map.
func TestMakeArrayChildrenEmpty(t *testing.T) {
	children := makeArrayChildren(nil, "INT", 6)
	if len(children) != 0 {
		t.Errorf("expected 0 children for empty levels, got %d", len(children))
	}
}

// Validates: NO-SPEC (regression guard, awaiting spec backfill).
// Pins makeArrayChildren key indexing offset by LBound (LBound=5 → "[5]","[6]").
func TestMakeArrayChildrenNonZeroLBound(t *testing.T) {
	levels := []datatypeArrayInfo{{LBound: 5, Elements: 2}}
	children := makeArrayChildren(levels, "BYTE", 2)
	if _, ok := children["[5]"]; !ok {
		t.Error("expected child '[5]'")
	}
	if _, ok := children["[6]"]; !ok {
		t.Error("expected child '[6]'")
	}
}

// Validates: NO-SPEC (regression guard, awaiting spec backfill).
// Pins 2-D array hierarchical child generation (outer dim has 3 sub-children each).
func TestMakeArrayChildren_2D(t *testing.T) {
	// ARRAY[0..1, 0..2] OF INT — 2x3 = 6 elements, 12 bytes total
	levels := []datatypeArrayInfo{
		{LBound: 0, Elements: 2},
		{LBound: 0, Elements: 3},
	}
	children := makeArrayChildren(levels, "INT", 12)
	if len(children) != 2 {
		t.Fatalf("expected 2 top-level children, got %d", len(children))
	}
	for _, name := range []string{"[0]", "[1]"} {
		child, ok := children[name]
		if !ok {
			t.Errorf("missing child %s", name)
			continue
		}
		if len(child.Children) != 3 {
			t.Errorf("child %s: expected 3 sub-children, got %d", name, len(child.Children))
		}
		if child.DatatypeEntry.Size != 6 { // 12/2 = 6 bytes per row
			t.Errorf("child %s: size = %d, want 6", name, child.DatatypeEntry.Size)
		}
		// Inner-element Size must equal one INT (2 bytes), not the outer row size.
		// Regression guard for makeArrayChildren passing full size to recursion.
		for _, inner := range []string{"[0]", "[1]", "[2]"} {
			leaf, ok := child.Children[inner]
			if !ok {
				t.Errorf("child %s missing inner %s", name, inner)
				continue
			}
			if leaf.DatatypeEntry.Size != 2 {
				t.Errorf("child %s inner %s: size = %d, want 2", name, inner, leaf.DatatypeEntry.Size)
			}
		}
	}
}

// Validates: NO-SPEC (regression guard, awaiting spec backfill).
// Pins 3-D array nested child generation (2x2x2 leaf shape).
func TestMakeArrayChildren_3D(t *testing.T) {
	// ARRAY[0..1, 0..1, 0..1] OF BYTE — 2x2x2 = 8 elements
	levels := []datatypeArrayInfo{
		{LBound: 0, Elements: 2},
		{LBound: 0, Elements: 2},
		{LBound: 0, Elements: 2},
	}
	children := makeArrayChildren(levels, "BYTE", 8)
	if len(children) != 2 {
		t.Fatalf("expected 2 children, got %d", len(children))
	}
	// Drill down to leaf level
	c0 := children["[0]"]
	if c0 == nil {
		t.Fatal("missing [0]")
	}
	if len(c0.Children) != 2 {
		t.Fatalf("[0] expected 2 children, got %d", len(c0.Children))
	}
	c00 := c0.Children["[0]"]
	if c00 == nil {
		t.Fatal("missing [0][0]")
	}
	if len(c00.Children) != 2 {
		t.Fatalf("[0][0] expected 2 children, got %d", len(c00.Children))
	}
	// Leaf-element Size must equal one BYTE (1 byte), not the parent slab.
	// Regression guard for makeArrayChildren passing full size to recursion.
	c000 := c00.Children["[0]"]
	if c000 == nil {
		t.Fatal("missing [0][0][0]")
	}
	if c000.DatatypeEntry.Size != 1 {
		t.Errorf("[0][0][0] size = %d, want 1", c000.DatatypeEntry.Size)
	}
}

// Validates: NO-SPEC (regression guard, awaiting spec backfill).
// Pins zero-length array → empty children map.
func TestMakeArrayChildren_ZeroElements(t *testing.T) {
	levels := []datatypeArrayInfo{{LBound: 0, Elements: 0}}
	children := makeArrayChildren(levels, "INT", 0)
	if len(children) != 0 {
		t.Errorf("expected 0 children for zero elements, got %d", len(children))
	}
}

// --- inferBaseType ---

// Validates: NO-SPEC (regression guard, awaiting spec backfill).
// Pins size→base-type mapping: only 1 (SINT) and 2 (INT) are inferred.
// 4 and 8 are deliberately refused regardless of BaseType — REAL/LREAL
// share those widths with DINT/LINT and the byte layout cannot be
// disambiguated without a datatype table. baseType is accepted for
// future use but currently does not affect the result.
func TestInferBaseType(t *testing.T) {
	tests := []struct {
		size     uint32
		baseType ADSDataType
		want     string
	}{
		{1, 0, "SINT"},
		{2, 0, "INT"},
		{4, 0, ""},           // refused: REAL/DINT ambiguity
		{8, 0, ""},           // refused: LREAL/LINT ambiguity
		{4, ADSTBigType, ""}, // refused even for BIGTYPE — caller must LoadSymbols
		{8, ADSTBigType, ""},
		{3, 0, ""},
		{16, 0, ""},
		{0, 0, ""},
	}
	for _, tt := range tests {
		got := inferBaseType(tt.size, tt.baseType)
		if got != tt.want {
			t.Errorf("inferBaseType(size=%d, baseType=%d) = %q, want %q", tt.size, tt.baseType, got, tt.want)
		}
	}
}

// --- GetJSON ---

// Validates: NO-SPEC (regression guard, awaiting spec backfill).
// Pins Sym.GetJSON for plain INT scalar (raw numeric literal, no quoting).
func TestGetJSON(t *testing.T) {
	sym := &Symbol{DataType: "INT", Length: 2, Value: "42"}
	json := sym.GetJSON()
	if json != "42" {
		t.Errorf("got %q, want %q", json, "42")
	}
}

// Validates: NO-SPEC (regression guard, awaiting spec backfill).
// Pins BOOL JSON encoding (raw "true"/"false", no quoting).
func TestGetJSONBool(t *testing.T) {
	sym := &Symbol{DataType: "BOOL", Length: 1, Value: "true"}
	json := sym.GetJSON()
	if json != "true" {
		t.Errorf("got %q, want %q", json, "true")
	}
}

// Validates: NO-SPEC (regression guard, awaiting spec backfill).
// Pins STRING JSON quoting (value wrapped in double-quotes).
func TestGetJSONString(t *testing.T) {
	sym := &Symbol{DataType: "STRING", Length: 20, Value: "hello"}
	json := sym.GetJSON()
	if json != `"hello"` {
		t.Errorf("got %q, want %q", json, `"hello"`)
	}
}

// Validates: NO-SPEC (regression guard, awaiting spec backfill).
// Pins struct JSON encoding via nested-children traversal.
func TestGetJSONStruct(t *testing.T) {
	child := &Symbol{Name: "x", FullName: "s.x", DataType: "INT", Length: 2, Value: "10"}
	parent := &Symbol{
		Name: "s", FullName: "s", DataType: "ST_S", Length: 2,
		Children: map[string]*Symbol{"x": child},
	}
	json := parent.GetJSON()
	if !strings.Contains(json, "10") {
		t.Errorf("expected JSON to contain value 10, got %q", json)
	}
}

// Validates: NO-SPEC (regression guard, awaiting spec backfill).
// Pins empty STRING value → JSON empty-quoted string `""`.
func TestGetJSON_EmptyValue(t *testing.T) {
	sym := &Symbol{DataType: "STRING", Length: 10, Value: ""}
	json := sym.GetJSON()
	if json != `""` {
		t.Errorf("got %q, want %q", json, `""`)
	}
}

// Validates: NO-SPEC.
func TestGetJSON_WSTRINGAsString(t *testing.T) {
	sym := &Symbol{
		Name:     "MyWString",
		FullName: "GVL.MyWString",
		DataType: "WSTRING",
		Value:    "Hello World",
	}
	got := sym.GetJSON()
	if got != `"Hello World"` {
		t.Errorf("expected WSTRING as JSON string, got %s", got)
	}
}

// --- parseUploadSymbolInfoSymbols with real data ---

// Validates: R-CACHE-006 (case-insensitive key).
func TestParseUploadSymbolInfoSymbols_SingleSymbol(t *testing.T) {
	// Build a minimal symbol entry: header + name + null + type + null + comment + null
	name := []byte("MAIN.test")
	dt := []byte("INT")
	comment := []byte("")

	entry := symbolEntry{
		IGroup:        0x4020,
		IOffs:         0,
		Size:          2,
		DataType:      0,
		Flags:         0,
		NameLength:    uint16(len(name)),
		TypeLength:    uint16(len(dt)),
		CommentLength: uint16(len(comment)),
	}
	// EntryLength = sizeof(symbolEntry[binary]) + name + 1 + dt + 1 + comment + 1
	// binary.Write packs symbolEntry to 30 bytes (6×uint32 + 3×uint16)
	entryLen := 30 + len(name) + 1 + len(dt) + 1 + len(comment) + 1
	entry.EntryLength = uint32(entryLen)

	buf := make([]byte, 0, entryLen)
	b := make([]byte, 30)
	binary.LittleEndian.PutUint32(b[0:], entry.EntryLength)
	binary.LittleEndian.PutUint32(b[4:], entry.IGroup)
	binary.LittleEndian.PutUint32(b[8:], entry.IOffs)
	binary.LittleEndian.PutUint32(b[12:], entry.Size)
	binary.LittleEndian.PutUint32(b[16:], entry.DataType)
	binary.LittleEndian.PutUint32(b[20:], entry.Flags)
	// The last 6 bytes need special packing for uint16 fields
	// Actually symbolEntry is a packed struct; let's use binary.Write
	var entryBuf bytes.Buffer
	if err := binary.Write(&entryBuf, binary.LittleEndian, entry); err != nil {
		t.Fatalf("binary.Write failed: %v", err)
	}
	buf = append(buf, entryBuf.Bytes()...)
	buf = append(buf, name...)
	buf = append(buf, 0) // null terminator
	buf = append(buf, dt...)
	buf = append(buf, 0) // null terminator
	buf = append(buf, comment...)
	buf = append(buf, 0) // null terminator

	symbols, err := parseUploadSymbolInfoSymbols(buf, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(symbols) != 1 {
		t.Fatalf("expected 1 symbol, got %d", len(symbols))
	}
	sym, ok := symbols["main.test"] // internal keys are lowercased
	if !ok {
		t.Fatal("expected symbol 'main.test'")
	}
	if sym.FullName != "MAIN.test" {
		t.Errorf("FullName = %q, want %q (PLC original casing)", sym.FullName, "MAIN.test")
	}
	if sym.DataType != "INT" {
		t.Errorf("DataType = %q, want %q", sym.DataType, "INT")
	}
	if sym.Length != 2 {
		t.Errorf("Length = %d, want 2", sym.Length)
	}
	if sym.Group != 0x4020 {
		t.Errorf("Group = 0x%x, want 0x4020", sym.Group)
	}
}

// Validates: R-CMD-007.
func TestParseUploadSymbolInfoSymbols_TruncatedEntry(t *testing.T) {
	// Only 10 bytes — not enough for a symbolEntry header
	_, err := parseUploadSymbolInfoSymbols([]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, nil)
	if err == nil {
		t.Error("expected error for truncated data")
	}
}

// --- symbolSumAddress ---

// Validates: R-SUM-008.
func TestSymbolSumAddress_PrefersHandleOverDirect(t *testing.T) {
	// Handle-based addressing preferred for sum commands because direct
	// process image addressing (0x4040) fails inside sum reads on some PLCs.
	sym := &Symbol{
		Group:  0x4020,
		Offset: 0x1234,
		Handle: 0xABCD,
		Length: 4,
	}
	group, offset := symbolSumAddress(sym)
	if group != uint32(GroupSymbolValueByHandle) {
		t.Errorf("group = 0x%X, want 0x%X (GroupSymbolValueByHandle)", group, uint32(GroupSymbolValueByHandle))
	}
	if offset != 0xABCD {
		t.Errorf("offset = 0x%X, want 0xABCD (handle)", offset)
	}
}

// Validates: R-SUM-008.
func TestSymbolSumAddress_HandleOnlyNoGroup(t *testing.T) {
	// Handle-based when Group is 0
	sym := &Symbol{
		Group:  0,
		Offset: 0,
		Handle: 0xABCD,
		Length: 4,
	}
	group, offset := symbolSumAddress(sym)
	if group != uint32(GroupSymbolValueByHandle) {
		t.Errorf("group = 0x%X, want 0x%X (GroupSymbolValueByHandle)", group, uint32(GroupSymbolValueByHandle))
	}
	if offset != 0xABCD {
		t.Errorf("offset = 0x%X, want 0xABCD (handle)", offset)
	}
}

// Validates: R-SUM-008.
func TestSymbolSumAddress_DirectFallbackNoHandle(t *testing.T) {
	// Falls back to direct group/offset when no handle is available
	sym := &Symbol{
		Group:  0x4020,
		Offset: 0x0100,
		Handle: 0,
		Length: 2,
	}
	group, offset := symbolSumAddress(sym)
	if group != 0x4020 {
		t.Errorf("group = 0x%X, want 0x4020", group)
	}
	if offset != 0x0100 {
		t.Errorf("offset = 0x%X, want 0x0100", offset)
	}
}

// Validates: R-SUM-008.
func TestSymbolSumAddress_DirectFallbackChildAccumulatesOffset(t *testing.T) {
	// Without handles, child symbols accumulate offsets from parent chain.
	parent := &Symbol{
		Group:  0x4040,
		Offset: 0x1000, // absolute offset in PLC memory
		Handle: 0,
		Length: 100,
	}
	child := &Symbol{
		Group:  0x4040,
		Offset: 0x0010, // relative offset within parent struct
		Handle: 0,
		Length: 4,
		Parent: parent,
	}
	group, offset := symbolSumAddress(child)
	if group != 0x4040 {
		t.Errorf("group = 0x%X, want 0x4040", group)
	}
	if offset != 0x1010 { // 0x1000 + 0x0010
		t.Errorf("offset = 0x%X, want 0x1010 (parent 0x1000 + child 0x0010)", offset)
	}
}

// Validates: R-SUM-008.
func TestSymbolSumAddress_DirectFallbackNestedChild(t *testing.T) {
	// Deeply nested symbol without handles: grandparent → parent → child
	grandparent := &Symbol{
		Group:  0x4040,
		Offset: 0x2000, // absolute
		Handle: 0,
		Length: 200,
	}
	parent := &Symbol{
		Group:  0x4040,
		Offset: 0x0080, // relative within grandparent
		Handle: 0,
		Length: 50,
		Parent: grandparent,
	}
	child := &Symbol{
		Group:  0x4040,
		Offset: 0x0004, // relative within parent
		Handle: 0,
		Length: 2,
		Parent: parent,
	}
	group, offset := symbolSumAddress(child)
	if group != 0x4040 {
		t.Errorf("group = 0x%X, want 0x4040", group)
	}
	// 0x2000 + 0x0080 + 0x0004 = 0x2084
	if offset != 0x2084 {
		t.Errorf("offset = 0x%X, want 0x2084 (0x2000 + 0x0080 + 0x0004)", offset)
	}
}

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

func TestParseUploadSymbolInfoSymbols_EntryLengthTooShort(t *testing.T) {
	// Build a symbol entry where EntryLength is smaller than the actual
	// bytes consumed by the fixed-size header + name/type/comment.
	// symbolEntry is 30 bytes; NameLength=5, TypeLength=4, CommentLength=0.
	// Actual consumed = 30 + 5+1 + 4+1 + 0+1 = 42 bytes.
	// Set EntryLength = 10 (too short) to trigger skip < 0.
	buf := new(bytes.Buffer)
	entry := symbolEntry{
		EntryLength:   10, // intentionally too small
		IGroup:        0x4020,
		IOffs:         0,
		Size:          4,
		DataType:      0,
		Flags:         0,
		NameLength:    5,
		TypeLength:    4,
		CommentLength: 0,
	}
	_ = binary.Write(buf, binary.LittleEndian, entry)
	buf.WriteString("MyVar") // name (5 bytes)
	buf.WriteByte(0)         // null terminator
	buf.WriteString("DINT")  // type (4 bytes)
	buf.WriteByte(0)         // null terminator
	buf.WriteByte(0)         // comment null terminator

	_, err := parseUploadSymbolInfoSymbols(buf.Bytes(), nil)
	if err == nil {
		t.Fatal("expected error for EntryLength shorter than bytes consumed, got nil")
	}
}
