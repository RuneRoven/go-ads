package ads

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"slices"
	"strconv"
	"time"
	"unicode/utf16"
)

func (symbol *Symbol) parse(data []byte, offset int, datatypes map[string]SymbolUploadDataType) (string, error) {
	start := offset
	// F-16: reject oversized symbol.Length before arithmetic. uint32 → int
	// conversion wraps on 32-bit Go; an attacker-controlled or buggy symbol
	// entry with Length = 0xFFFFFFFF produces a negative int that bypasses
	// the bounds guard and panics on slice. Compare in uint64 space to dodge
	// the wrap entirely.
	if uint64(symbol.Length) > uint64(len(data)) {
		return "", fmt.Errorf("parse %s: symbol.Length %d exceeds data buffer size %d", symbol.DataType, symbol.Length, len(data))
	}
	stop := start + int(symbol.Length)
	if start+int(symbol.Length) > len(data) {
		stop = len(data)
	}

	var newValue string
	if len(symbol.Children) > 0 {
		for _, value := range symbol.Children {
			if _, err := value.parse(data[offset:stop], int(value.Offset), datatypes); err != nil {
				return "", fmt.Errorf("parsing child %q: %w", value.Name, err)
			}
		}
		newValue = symbol.GetJSON(false)
		symbol.updateValue(newValue)
		return symbol.Value, nil
	}

	if start+int(symbol.Length) > len(data) {
		return "", fmt.Errorf("data too short for %s at offset %d: need %d bytes, got %d", symbol.DataType, start, symbol.Length, len(data)-start)
	}

	switch symbol.DataType {
	case "BOOL":
		if stop-start != 1 {
			return "", fmt.Errorf("BOOL Size Wrong")
		}
		if data[start:stop][0] > 0 {
			newValue = "true"
		} else {
			newValue = "false"
		}
	case "BYTE", "USINT": // Unsigned Short INT 0 to 255
		if stop-start != 1 {
			return "", fmt.Errorf("BYTE Size Wrong")
		}
		newValue = strconv.FormatUint(uint64(data[start]), 10)
	case "SINT": // Short INT -128 to 127
		if stop-start != 1 {
			return "", fmt.Errorf("SINT Size Wrong")
		}
		newValue = strconv.FormatInt(int64(int8(data[start])), 10)
	case "UINT", "WORD", "UINT16":
		if stop-start != 2 {
			return "", fmt.Errorf("WORD Size Wrong")
		}
		i := binary.LittleEndian.Uint16(data[start:stop])
		newValue = strconv.FormatUint(uint64(i), 10)
	case "UDINT", "DWORD":
		if stop-start != 4 {
			return "", fmt.Errorf("DWORD Size Wrong")
		}
		i := binary.LittleEndian.Uint32(data[start:stop])
		newValue = strconv.FormatUint(uint64(i), 10)
	case "INT", "INT16":
		if stop-start != 2 {
			return "", fmt.Errorf("INT Size Wrong")
		}
		i := int16(binary.LittleEndian.Uint16(data[start:stop]))
		newValue = strconv.FormatInt(int64(i), 10)
	case "DINT":
		if stop-start != 4 {
			return "", fmt.Errorf("DINT Size Wrong")
		}
		i := int32(binary.LittleEndian.Uint32(data[start:stop]))
		newValue = strconv.FormatInt(int64(i), 10)
	case "REAL":
		if stop-start != 4 {
			return "", fmt.Errorf("REAL Size Wrong")
		}
		i := binary.LittleEndian.Uint32(data[start:stop])
		f := math.Float32frombits(i)
		newValue = strconv.FormatFloat(float64(f), 'f', -1, 32)
	case "LREAL":
		if stop-start != 8 {
			return "", fmt.Errorf("LREAL Size Wrong")
		}
		i := binary.LittleEndian.Uint64(data[start:stop])
		f := math.Float64frombits(i)
		newValue = strconv.FormatFloat(f, 'f', -1, 64)
	case "LINT":
		if stop-start != 8 {
			return "", fmt.Errorf("LINT Size Wrong")
		}
		i := int64(binary.LittleEndian.Uint64(data[start:stop]))
		newValue = strconv.FormatInt(i, 10)
	case "ULINT", "LWORD":
		if stop-start != 8 {
			return "", fmt.Errorf("ULINT Size Wrong")
		}
		i := binary.LittleEndian.Uint64(data[start:stop])
		newValue = strconv.FormatUint(i, 10)
	case "STRING":
		raw := data[start:stop]
		idx := bytes.IndexByte(raw, 0)
		if idx < 0 {
			idx = len(raw)
		}
		newValue = string(raw[:idx])
	case "WSTRING":
		raw := data[start:stop]
		// Find UTF-16LE null terminator (0x0000 on 2-byte boundary)
		n := len(raw) &^ 1 // round down to even
		for i := 0; i+1 < len(raw); i += 2 {
			if raw[i] == 0 && raw[i+1] == 0 {
				n = i
				break
			}
		}
		runes := make([]uint16, n/2)
		for i := 0; i < n; i += 2 {
			runes[i/2] = binary.LittleEndian.Uint16(raw[i:])
		}
		newValue = string(utf16.Decode(runes))
	case "TIME":
		if stop-start != 4 {
			return "", fmt.Errorf("TIME Size Wrong")
		}
		i := binary.LittleEndian.Uint32(data[start:stop])
		t := time.Unix(0, int64(uint64(i)*uint64(time.Millisecond))).UTC()

		newValue = t.Truncate(time.Millisecond).Format("15:04:05.999999999")
	case "TOD", "TIME_OF_DAY":
		if stop-start != 4 {
			return "", fmt.Errorf("TOD Size Wrong")
		}
		i := binary.LittleEndian.Uint32(data[start:stop])
		t := time.Unix(0, int64(uint64(i)*uint64(time.Millisecond))).UTC()

		newValue = t.Truncate(time.Millisecond).Format("15:04")
	case "DATE":
		if stop-start != 4 {
			return "", fmt.Errorf("DATE Size Wrong")
		}
		i := binary.LittleEndian.Uint32(data[start:stop])
		t := time.Unix(int64(i), 0).UTC()

		newValue = t.Format("2006-01-02")
	case "DT", "DATE_AND_TIME":
		if stop-start != 4 {
			return "", fmt.Errorf("DT Size Wrong")
		}
		i := binary.LittleEndian.Uint32(data[start:stop])
		t := time.Unix(int64(i), 0).UTC()

		newValue = t.Truncate(time.Millisecond).Format("2006-01-02 15:04:05")
	default:
		// Try resolving type alias via datatype table (enums, type aliases)
		if datatypes != nil {
			if dt, ok := datatypes[symbol.DataType]; ok {
				if slices.Contains(parseableTypes, dt.DataType) {
					resolved := *symbol
					resolved.DataType = dt.DataType
					val, err := resolved.parse(data, offset, nil)
					if err != nil {
						return "", err
					}
					symbol.updateValue(val)
					return symbol.Value, nil
				}
			}
		}
		// Use ADST_ numeric type code from protocol (authoritative).
		// The PLC sends the correct base type (e.g., ADSTReal32=4 for a REAL-based alias).
		if resolved := adsTypeToString(symbol.BaseType); resolved != "" {
			copy := *symbol
			copy.DataType = resolved
			val, err := copy.parse(data, offset, nil)
			if err != nil {
				return "", err
			}
			symbol.updateValue(val)
			return symbol.Value, nil
		}
		// Last resort: infer base type from symbol size when ADST_ code is
		// unavailable (BaseType=0). Handles enums and simple type aliases
		// whose underlying type matches a standard integer size.
		// WARNING: always infers signed integer types — unsigned enums and
		// REAL/LREAL types will be misinterpreted.
		if inferred := inferBaseType(symbol.Length); inferred != "" {
			getDefaultLogger().Warn("inferring base type from size (may be wrong for unsigned/float types)",
				"symbol", symbol.DataType, "size", symbol.Length, "inferred", inferred)
			resolved := *symbol
			resolved.DataType = inferred
			val, err := resolved.parse(data, offset, nil)
			if err != nil {
				return "", err
			}
			symbol.updateValue(val)
			return symbol.Value, nil
		}
		return "", fmt.Errorf("unknown format cannot parse: %s", symbol.DataType)
	}

	symbol.updateValue(newValue)
	return symbol.Value, nil
}

func (symbol *Symbol) updateValue(newValue string) {
	if symbol.Value != newValue &&
		(!symbol.ValueParsed || time.Since(symbol.LastUpdateTime) > symbol.MinUpdateInterval) {
		symbol.LastUpdateTime = time.Now()
		symbol.Value = newValue
		symbol.Valid = true
		symbol.ValueParsed = true
		symbol.Changed = true
		symbol.parentChanged()
	}
}

func (symbol *Symbol) parentChanged() {
	if symbol.Parent != nil {
		symbol.Parent.parentChanged()
	}
	symbol.Changed = true
}

var parseableTypes = []string{
	"BOOL",
	"BYTE",
	"USINT",
	"UINT",
	"UINT16",
	"WORD",
	"UDINT",
	"DWORD",
	"SINT",
	"INT",
	"INT16",
	"DINT",
	"REAL",
	"LREAL",
	"STRING",
	"WSTRING",
	"TIME",
	"TOD",
	"TIME_OF_DAY",
	"DATE",
	"DT",
	"DATE_AND_TIME",
	"LINT",
	"ULINT",
	"LWORD",
}

// inferBaseType guesses a parseable base type from a symbol's byte size.
// Used as a last resort when the datatype table is unavailable (on-demand mode)
// to parse enums and simple type aliases. Returns "" if no match.
func inferBaseType(size uint32) string {
	switch size {
	case 1:
		return "SINT"
	case 2:
		return "INT"
	case 4:
		return "DINT"
	case 8:
		return "LINT"
	default:
		return ""
	}
}

func (symbol *Symbol) writeToNode(value string, offset int, datatypes map[string]SymbolUploadDataType) (data []byte, err error) {
	if len(symbol.Children) > 0 {
		var fields map[string]json.RawMessage
		if err := json.Unmarshal([]byte(value), &fields); err != nil {
			return nil, fmt.Errorf("struct write requires JSON input: %w", err)
		}
		buf := make([]byte, symbol.Length)
		for name, child := range symbol.Children {
			raw, ok := fields[name]
			if !ok {
				continue
			}
			// Convert JSON value to string for writeToNode:
			// - JSON strings ("hello") → unquoted (hello)
			// - numbers/bools (42, true) → raw text (42, true)
			var childValue string
			if len(raw) > 0 && raw[0] == '"' {
				if err := json.Unmarshal(raw, &childValue); err != nil {
					return nil, fmt.Errorf("field %q: invalid JSON string: %w", name, err)
				}
			} else {
				childValue = string(raw)
			}
			childBytes, err := child.writeToNode(childValue, 0, datatypes)
			if err != nil {
				return nil, fmt.Errorf("field %q: %w", name, err)
			}
			end := child.Offset + child.Length
			if end > uint32(len(buf)) {
				return nil, fmt.Errorf("field %q: offset+length %d exceeds struct size %d", name, end, len(buf))
			}
			copy(buf[child.Offset:end], childBytes)
		}
		return buf, nil
	}

	buf := new(bytes.Buffer)
	dt := symbol.DataType

	if !slices.Contains(parseableTypes, dt) {
		if datatypes == nil {
			return nil, fmt.Errorf("cannot write to symbol with aliased type %q without full symbol discovery; call LoadSymbols() first", symbol.DataType)
		}
		dtEntry, ok := datatypes[dt]
		if !ok {
			return nil, fmt.Errorf("datatype %q not found in datatype table", dt)
		}
		if slices.Contains(parseableTypes, dtEntry.DataType) {
			dt = dtEntry.DataType
		} else {
			return nil, fmt.Errorf("data type not parseable: %s", dtEntry.DataType)
		}
	}
	switch dt {
	case "BOOL":
		v, e := strconv.ParseBool(value)
		if e != nil {
			return nil, e
		}

		if v {
			buf.WriteByte(1)
		} else {
			buf.WriteByte(0)
		}
	case "BYTE", "USINT": // Unsigned Short INT 0 to 255
		v, e := strconv.ParseUint(value, 10, 8)
		if e != nil {
			return nil, e
		}
		buf.WriteByte(uint8(v))
	case "UINT", "WORD", "UINT16":
		v, e := strconv.ParseUint(value, 10, 16)
		if e != nil {
			return nil, e
		}

		v16 := uint16(v)
		if err := binary.Write(buf, binary.LittleEndian, &v16); err != nil {
			return nil, fmt.Errorf("binary.Write UINT failed: %w", err)
		}
	case "UDINT", "DWORD":
		v, e := strconv.ParseUint(value, 10, 32)
		if e != nil {
			return nil, e
		}

		v32 := uint32(v)
		if err := binary.Write(buf, binary.LittleEndian, &v32); err != nil {
			return nil, fmt.Errorf("binary.Write UDINT failed: %w", err)
		}

	case "SINT": // Short INT -128 to 127
		v, e := strconv.ParseInt(value, 10, 8)
		if e != nil {
			return nil, e
		}
		buf.WriteByte(byte(int8(v)))
	case "INT", "INT16":
		v, e := strconv.ParseInt(value, 10, 16)
		if e != nil {
			return nil, e
		}

		v16 := int16(v)
		if err := binary.Write(buf, binary.LittleEndian, &v16); err != nil {
			return nil, fmt.Errorf("binary.Write INT failed: %w", err)
		}
	case "DINT":
		v, e := strconv.ParseInt(value, 10, 32)
		if e != nil {
			return nil, e
		}

		v32 := int32(v)
		if err := binary.Write(buf, binary.LittleEndian, &v32); err != nil {
			return nil, fmt.Errorf("binary.Write DINT failed: %w", err)
		}

	case "REAL":
		v, e := strconv.ParseFloat(value, 32)
		if e != nil {
			return nil, e
		}

		v32 := math.Float32bits(float32(v))
		if err := binary.Write(buf, binary.LittleEndian, &v32); err != nil {
			return nil, fmt.Errorf("binary.Write REAL failed: %w", err)
		}
	case "LREAL":
		v, e := strconv.ParseFloat(value, 64)
		if e != nil {
			return nil, e
		}

		v64 := math.Float64bits(v)
		if err := binary.Write(buf, binary.LittleEndian, &v64); err != nil {
			return nil, fmt.Errorf("binary.Write LREAL failed: %w", err)
		}
	case "LINT":
		v, e := strconv.ParseInt(value, 10, 64)
		if e != nil {
			return nil, e
		}
		if err := binary.Write(buf, binary.LittleEndian, &v); err != nil {
			return nil, fmt.Errorf("binary.Write LINT failed: %w", err)
		}
	case "ULINT", "LWORD":
		v, e := strconv.ParseUint(value, 10, 64)
		if e != nil {
			return nil, e
		}
		if err := binary.Write(buf, binary.LittleEndian, &v); err != nil {
			return nil, fmt.Errorf("binary.Write ULINT failed: %w", err)
		}
	case "TIME":
		t, e := time.Parse("15:04:05.999999999", value)
		if e != nil {
			t, e = time.Parse("15:04:05", value)
			if e != nil {
				return nil, fmt.Errorf("TIME: expected format 15:04:05 or 15:04:05.999999999: %w", e)
			}
		}
		target := time.Date(1970, 1, 1, t.Hour(), t.Minute(), t.Second(), t.Nanosecond(), time.UTC)
		ms := uint32(target.UnixNano() / int64(time.Millisecond))
		if err := binary.Write(buf, binary.LittleEndian, &ms); err != nil {
			return nil, fmt.Errorf("binary.Write TIME failed: %w", err)
		}
	case "TOD", "TIME_OF_DAY":
		t, e := time.Parse("15:04", value)
		if e != nil {
			return nil, fmt.Errorf("TOD: expected format 15:04: %w", e)
		}
		target := time.Date(1970, 1, 1, t.Hour(), t.Minute(), 0, 0, time.UTC)
		ms := uint32(target.UnixNano() / int64(time.Millisecond))
		if err := binary.Write(buf, binary.LittleEndian, &ms); err != nil {
			return nil, fmt.Errorf("binary.Write TOD failed: %w", err)
		}
	case "DATE":
		t, e := time.Parse("2006-01-02", value)
		if e != nil {
			return nil, fmt.Errorf("DATE: expected format 2006-01-02: %w", e)
		}
		// Inverse of parse(): parse uses time.Unix(0, i*second) then Format in local TZ
		target := time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)
		secs := uint32(target.Unix())
		if err := binary.Write(buf, binary.LittleEndian, &secs); err != nil {
			return nil, fmt.Errorf("binary.Write DATE failed: %w", err)
		}
	case "DT", "DATE_AND_TIME":
		t, e := time.Parse("2006-01-02 15:04:05", value)
		if e != nil {
			return nil, fmt.Errorf("DT: expected format 2006-01-02 15:04:05: %w", e)
		}
		target := time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second(), 0, time.UTC)
		secs := uint32(target.Unix())
		if err := binary.Write(buf, binary.LittleEndian, &secs); err != nil {
			return nil, fmt.Errorf("binary.Write DT failed: %w", err)
		}
	case "STRING":
		// F-17: refuse to write to a STRING declared with Length=0. The PLC
		// expects at least 1 byte for the null terminator; a 0-byte payload
		// is silently truncating and indistinguishable from caller error.
		if symbol.Length < 1 {
			return nil, fmt.Errorf("STRING write requires symbol.Length >= 1, got %d", symbol.Length)
		}
		newBuf := make([]byte, symbol.Length)
		// Reserve last byte for null terminator — PLC expects null-terminated strings.
		maxLen := int(symbol.Length) - 1
		src := []byte(value)
		if len(src) > maxLen {
			src = src[:maxLen]
		}
		copy(newBuf, src)
		buf.Write(newBuf)
	case "WSTRING":
		// F-17: WSTRING needs at least 2 bytes for the UTF-16 null terminator.
		if symbol.Length < 2 {
			return nil, fmt.Errorf("WSTRING write requires symbol.Length >= 2, got %d", symbol.Length)
		}
		encoded := utf16.Encode([]rune(value))
		newBuf := make([]byte, symbol.Length)
		maxChars := (int(symbol.Length) - 2) / 2 // reserve 2 bytes for null terminator
		if len(encoded) > maxChars {
			encoded = encoded[:maxChars]
		}
		for i, r := range encoded {
			binary.LittleEndian.PutUint16(newBuf[i*2:], r)
		}
		buf.Write(newBuf)
	default:
		return nil, fmt.Errorf("datatype %q write is not implemented yet", symbol.DataType)
	}
	return buf.Bytes(), nil
}

// ReadBit extracts a single bit from a byte slice.
// bitIndex 0 is the least significant bit of the first byte.
// Returns false if bitIndex is out of range.
func ReadBit(data []byte, bitIndex int) bool {
	byteIdx := bitIndex / 8
	if bitIndex < 0 || byteIdx >= len(data) {
		return false
	}
	bitIdx := bitIndex % 8
	return data[byteIdx]&(1<<uint(bitIdx)) != 0
}

// WriteBit sets or clears a single bit in a byte slice.
// bitIndex 0 is the least significant bit of the first byte.
// Does nothing if bitIndex is out of range.
func WriteBit(data []byte, bitIndex int, value bool) {
	byteIdx := bitIndex / 8
	if bitIndex < 0 || byteIdx >= len(data) {
		return
	}
	bitIdx := bitIndex % 8
	if value {
		data[byteIdx] |= 1 << uint(bitIdx)
	} else {
		data[byteIdx] &^= 1 << uint(bitIdx)
	}
}
