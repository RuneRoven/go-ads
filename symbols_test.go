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

func TestParseEnumWithoutDatatypes(t *testing.T) {
	// When datatypes table is nil (on-demand mode), enum types should still
	// parse by inferring the base type from the symbol's byte size.
	tests := []struct {
		name     string
		dataType string
		size     uint32
		data     []byte
		want     string
	}{
		{"1-byte enum", "E_SmallState", 1, []byte{42}, "42"},
		{"2-byte enum", "E_WordState", 2, func() []byte { b := make([]byte, 2); binary.LittleEndian.PutUint16(b, 1500); return b }(), "1500"},
		{"4-byte enum DINT", "E_MachineState", 4, func() []byte { b := make([]byte, 4); binary.LittleEndian.PutUint32(b, 2); return b }(), "2"},
		{"4-byte enum negative", "E_MachineState", 4, func() []byte {
			b := make([]byte, 4)
			v := int32(-1)
			binary.LittleEndian.PutUint32(b, uint32(v))
			return b
		}(), "-1"},
		{"8-byte enum", "E_BigState", 8, func() []byte { b := make([]byte, 8); binary.LittleEndian.PutUint64(b, 99); return b }(), "99"},
	}
	for _, tt := range tests {
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

func TestMakeArrayChildrenEmpty(t *testing.T) {
	children := makeArrayChildren(nil, "INT", 6)
	if len(children) != 0 {
		t.Errorf("expected 0 children for empty levels, got %d", len(children))
	}
}

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
	}
}

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
}

func TestMakeArrayChildren_ZeroElements(t *testing.T) {
	levels := []datatypeArrayInfo{{LBound: 0, Elements: 0}}
	children := makeArrayChildren(levels, "INT", 0)
	if len(children) != 0 {
		t.Errorf("expected 0 children for zero elements, got %d", len(children))
	}
}

// --- inferBaseType ---

func TestInferBaseType(t *testing.T) {
	tests := []struct {
		size uint32
		want string
	}{
		{1, "SINT"},
		{2, "INT"},
		{4, "DINT"},
		{8, "LINT"},
		{3, ""},
		{16, ""},
		{0, ""},
	}
	for _, tt := range tests {
		got := inferBaseType(tt.size)
		if got != tt.want {
			t.Errorf("inferBaseType(%d) = %q, want %q", tt.size, got, tt.want)
		}
	}
}

// --- GetJSON ---

func TestGetJSON(t *testing.T) {
	sym := &Symbol{DataType: "INT", Length: 2, Value: "42"}
	json := sym.GetJSON()
	if json != "42" {
		t.Errorf("got %q, want %q", json, "42")
	}
}

func TestGetJSONBool(t *testing.T) {
	sym := &Symbol{DataType: "BOOL", Length: 1, Value: "true"}
	json := sym.GetJSON()
	if json != "true" {
		t.Errorf("got %q, want %q", json, "true")
	}
}

func TestGetJSONString(t *testing.T) {
	sym := &Symbol{DataType: "STRING", Length: 20, Value: "hello"}
	json := sym.GetJSON()
	if json != `"hello"` {
		t.Errorf("got %q, want %q", json, `"hello"`)
	}
}

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

func TestGetJSON_EmptyValue(t *testing.T) {
	sym := &Symbol{DataType: "STRING", Length: 10, Value: ""}
	json := sym.GetJSON()
	if json != `""` {
		t.Errorf("got %q, want %q", json, `""`)
	}
}

func TestGetJSON_NumericOverflow(t *testing.T) {
	// GetJSON parses Value via strconv.ParseFloat then encodes as JSON
	// number. For ULINT max (2^64-1) this is lossy: float64 rounds to
	// the nearest representable value (1.8446744073709552e+19, which is
	// 2^64 = 18446744073709551616, ONE more than the input). The test
	// pins this documented limitation: any change to the encoding path
	// (e.g. switching to a JSON string for large integers) should
	// surface here as an explicit failure rather than silently changing
	// observable output.
	sym := &Symbol{DataType: "ULINT", Length: 8, Value: "18446744073709551615"}
	got := sym.GetJSON()
	const wantLossy = "18446744073709552000"
	if got != wantLossy {
		t.Errorf("ULINT max GetJSON() = %q, want %q (lossy float64 round-trip — fix when integer-as-string encoding lands)", got, wantLossy)
	}
}

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
	// EntryLength = sizeof(symbolEntry) + name + 1 + dt + 1 + comment + 1
	entryLen := 26 + len(name) + 1 + len(dt) + 1 + len(comment) + 1
	entry.EntryLength = uint32(entryLen)

	buf := make([]byte, 0, entryLen)
	b := make([]byte, 26)
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

func TestParseUploadSymbolInfoSymbols_TruncatedEntry(t *testing.T) {
	// Only 10 bytes — not enough for a symbolEntry header
	_, err := parseUploadSymbolInfoSymbols([]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, nil)
	if err == nil {
		t.Error("expected error for truncated data")
	}
}

// --- symbolSumAddress ---

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
