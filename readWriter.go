package ads

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"strconv"
	"time"

	"encoding/json"

	"github.com/rs/zerolog/log"
)

func (symbol *Symbol) parse(data []byte, offset int, datatypes map[string]SymbolUploadDataType) (string, error) {
	start := offset
	stop := start + int(symbol.Length)
	if start+int(symbol.Length) > len(data) {
		stop = len(data)
	}

	var newValue = "nil"
	if len(symbol.Childs) > 0 {
		for _, value := range symbol.Childs {
			value.parse(data[offset:stop], int(value.Offset), datatypes)
		}
		newValue = symbol.GetJSON(false)

	} else {
		if len(data) < int(symbol.Length) {
			log.Error().
				Msgf("Incoming data is to small, !0<%d<%d<%d", start, stop, len(data))
			return "", nil
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
			buf := bytes.NewBuffer(data[start:stop])
			var i uint8
			binary.Read(buf, binary.LittleEndian, &i)
			newValue = strconv.FormatInt(int64(i), 10)
		case "SINT": // Short INT -128 to 127
			if stop-start != 1 {
				return "", fmt.Errorf("SINT Size Wrong")
			}
			buf := bytes.NewBuffer(data[start:stop])
			var i int8
			binary.Read(buf, binary.LittleEndian, &i)
			newValue = strconv.FormatInt(int64(i), 10)
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
			var i int16
			i = int16(binary.LittleEndian.Uint16(data[start:stop]))
			newValue = strconv.FormatInt(int64(i), 10)
		case "DINT":
			if stop-start != 4 {
				return "", fmt.Errorf("DINT Size Wrong")
			}
			buf := bytes.NewBuffer(data[start:stop])
			var i int32
			binary.Read(buf, binary.LittleEndian, &i)
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
			trimmedBytes := bytes.TrimSpace(data[start:stop])
			secondIndex := bytes.IndexByte(trimmedBytes, byte(0))
			if secondIndex >= len(trimmedBytes) {
				secondIndex = len(trimmedBytes)
			}
			if secondIndex < 0 {
				secondIndex = len(trimmedBytes)
			}
			newValue = string(trimmedBytes[:(secondIndex)])
		case "TIME":
			if stop-start != 4 {
				return "", fmt.Errorf("TIME Size Wrong")
			}
			i := binary.LittleEndian.Uint32(data[start:stop])
			t := time.Unix(0, int64(uint64(i)*uint64(time.Millisecond))-int64(time.Hour))

			newValue = t.Truncate(time.Millisecond).Format("15:04:05.999999999")
		case "TOD":
			if stop-start != 4 {
				return "", fmt.Errorf("TOD Size Wrong")
			}
			i := binary.LittleEndian.Uint32(data[start:stop])
			t := time.Unix(0, int64(uint64(i)*uint64(time.Millisecond))-int64(time.Hour))

			newValue = t.Truncate(time.Millisecond).Format("15:04")
		case "DATE":
			if stop-start != 4 {
				return "", fmt.Errorf("DATE Size Wrong")
			}
			i := binary.LittleEndian.Uint32(data[start:stop])
			t := time.Unix(0, int64(uint64(i)*uint64(time.Second)))

			newValue = t.Truncate(time.Millisecond).Format("2006-01-02")
		case "DT":
			if stop-start != 4 {
				return "", fmt.Errorf("DT Size Wrong")
			}
			i := binary.LittleEndian.Uint32(data[start:stop])
			t := time.Unix(0, int64(uint64(i)*uint64(time.Second))-int64(time.Hour))

			newValue = t.Truncate(time.Millisecond).Format("2006-01-02 15:04:05")
		default:
			// Try resolving type alias via datatype table
			if datatypes != nil {
				if dt, ok := datatypes[symbol.DataType]; ok {
					if stringArrayIncludes(parseableTypes, dt.DataType) {
						resolved := *symbol
						resolved.DataType = dt.DataType
						return resolved.parse(data, offset, nil)
					}
				}
			}
			return "", fmt.Errorf("unknown format cannot parse: %s", symbol.DataType)
		}
	}
	if strcmp(symbol.Value, newValue) != 0 &&
		(symbol.Value == "" || time.Since(symbol.LastUpdateTime) > symbol.MinUpdateInterval) {
		symbol.LastUpdateTime = time.Now()
		symbol.Value = newValue
		symbol.Valid = true
		symbol.Changed = true
		symbol.parentChanged()
	}
	log.Trace().
		Str("value", newValue).
		Msg("parse value")
	return symbol.Value, nil
}

func (symbol *Symbol) parentChanged() {
	if symbol.Parent != nil {
		symbol.Parent.parentChanged()
	}
	symbol.Changed = true
}

func stringArrayIncludes(vs []string, t string) bool {
	for _, v := range vs {
		if v == t {
			return true
		}
	}
	return false
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
	"TIME",
	"TOD",
	"DATE",
	"DT",
	"LINT",
	"ULINT",
	"LWORD",
}

func (symbol *Symbol) writeToNode(value string, offset int, datatypes map[string]SymbolUploadDataType) (data []byte, err error) {
	if len(symbol.Childs) > 0 {
		var fields map[string]string
		if err := json.Unmarshal([]byte(value), &fields); err != nil {
			return nil, fmt.Errorf("struct write requires JSON input: %w", err)
		}
		buf := make([]byte, symbol.Length)
		for name, child := range symbol.Childs {
			childValue, ok := fields[name]
			if !ok {
				continue
			}
			childBytes, err := child.writeToNode(childValue, 0, datatypes)
			if err != nil {
				return nil, fmt.Errorf("field %q: %w", name, err)
			}
			copy(buf[child.Offset:child.Offset+child.Length], childBytes)
		}
		return buf, nil
	}

	buf := bytes.NewBuffer([]byte{})
	dt := symbol.DataType

	if !stringArrayIncludes(parseableTypes, symbol.DataType) {
		if datatypes == nil {
			return nil, fmt.Errorf("cannot write to symbol with aliased type %q without full symbol discovery; call LoadSymbols() first", symbol.DataType)
		}
		dtEntry, ok := datatypes[dt]
		if !ok {
			return nil, fmt.Errorf("datatype %q not found in datatype table", dt)
		}
		if stringArrayIncludes(parseableTypes, dtEntry.DataType) {
			dt = dtEntry.DataType
		} else {
			return nil, fmt.Errorf("data type not parseable %v", dtEntry.DataType)
		}
	}
	switch dt {
	case "BOOL":
		v, e := strconv.ParseBool(value)
		if e != nil {
			return nil, e
		}

		if v {
			buf.Write([]byte{1})
		} else {
			buf.Write([]byte{0})
		}
	case "BYTE", "USINT": // Unsigned Short INT 0 to 255
		v, e := strconv.ParseUint(value, 10, 8)
		if e != nil {
			return nil, e
		}

		v8 := uint8(v)
		binary.Write(buf, binary.LittleEndian, &v8)
	case "UINT", "WORD", "UINT16":
		v, e := strconv.ParseUint(value, 10, 16)
		if e != nil {
			return nil, e
		}

		v16 := uint16(v)
		binary.Write(buf, binary.LittleEndian, &v16)
	case "UDINT", "DWORD":
		v, e := strconv.ParseUint(value, 10, 32)
		if e != nil {
			return nil, e
		}

		v32 := uint32(v)
		binary.Write(buf, binary.LittleEndian, &v32)

	case "SINT": // Short INT -128 to 127
		v, e := strconv.ParseInt(value, 10, 8)
		if e != nil {
			return nil, e
		}

		v8 := int8(v)
		binary.Write(buf, binary.LittleEndian, &v8)
	case "INT", "INT16":
		v, e := strconv.ParseInt(value, 10, 16)
		if e != nil {
			return nil, e
		}

		v16 := int16(v)
		binary.Write(buf, binary.LittleEndian, &v16)
	case "DINT":
		v, e := strconv.ParseInt(value, 10, 32)
		if e != nil {
			return nil, e
		}

		v32 := int32(v)
		binary.Write(buf, binary.LittleEndian, &v32)

	case "REAL":
		v, e := strconv.ParseFloat(value, 32)
		if e != nil {
			return nil, e
		}

		v32 := math.Float32bits(float32(v))
		binary.Write(buf, binary.LittleEndian, &v32)
	case "LREAL":
		v, e := strconv.ParseFloat(value, 64)
		if e != nil {
			return nil, e
		}

		v64 := math.Float64bits(v)
		binary.Write(buf, binary.LittleEndian, &v64)
	case "LINT":
		v, e := strconv.ParseInt(value, 10, 64)
		if e != nil {
			return nil, e
		}
		v64 := int64(v)
		binary.Write(buf, binary.LittleEndian, &v64)
	case "ULINT", "LWORD":
		v, e := strconv.ParseUint(value, 10, 64)
		if e != nil {
			return nil, e
		}
		v64 := uint64(v)
		binary.Write(buf, binary.LittleEndian, &v64)
	case "TIME":
		t, e := time.Parse("15:04:05.999999999", value)
		if e != nil {
			t, e = time.Parse("15:04:05", value)
			if e != nil {
				return nil, fmt.Errorf("TIME: expected format 15:04:05 or 15:04:05.999999999: %w", e)
			}
		}
		// Inverse of parse(): parse uses time.Unix(0, i*ms - 1h) then Format in local TZ
		target := time.Date(1970, 1, 1, t.Hour(), t.Minute(), t.Second(), t.Nanosecond(), time.Local)
		ms := uint32((target.UnixNano() + int64(time.Hour)) / int64(time.Millisecond))
		binary.Write(buf, binary.LittleEndian, &ms)
	case "TOD":
		t, e := time.Parse("15:04", value)
		if e != nil {
			return nil, fmt.Errorf("TOD: expected format 15:04: %w", e)
		}
		target := time.Date(1970, 1, 1, t.Hour(), t.Minute(), 0, 0, time.Local)
		ms := uint32((target.UnixNano() + int64(time.Hour)) / int64(time.Millisecond))
		binary.Write(buf, binary.LittleEndian, &ms)
	case "DATE":
		t, e := time.Parse("2006-01-02", value)
		if e != nil {
			return nil, fmt.Errorf("DATE: expected format 2006-01-02: %w", e)
		}
		// Inverse of parse(): parse uses time.Unix(0, i*second) then Format in local TZ
		target := time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.Local)
		secs := uint32(target.Unix())
		binary.Write(buf, binary.LittleEndian, &secs)
	case "DT":
		t, e := time.Parse("2006-01-02 15:04:05", value)
		if e != nil {
			return nil, fmt.Errorf("DT: expected format 2006-01-02 15:04:05: %w", e)
		}
		// Inverse of parse(): parse uses time.Unix(0, i*second - 1h) then Format in local TZ
		target := time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second(), 0, time.Local)
		secs := uint32((target.UnixNano() + int64(time.Hour)) / int64(time.Second))
		binary.Write(buf, binary.LittleEndian, &secs)
	case "STRING":
		newBuf := make([]byte, symbol.Length)
		copy(newBuf, []byte(value))
		buf.Write(newBuf)
	default:
		err = fmt.Errorf("datatype '%s' write is not implemented yet", symbol.DataType)
		return
	}
	return buf.Bytes(), err
}

func strcmp(a, b string) int {
	min := len(b)
	if len(a) < len(b) {
		min = len(a)
	}
	diff := 0
	for i := 0; i < min && diff == 0; i++ {
		diff = int(a[i]) - int(b[i])
	}
	if diff == 0 {
		diff = len(a) - len(b)
	}
	return diff
}
