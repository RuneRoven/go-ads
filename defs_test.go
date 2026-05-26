package ads

import (
	"encoding/binary"
	"strings"
	"testing"
)

// --- ParseNetID ---

// Validates: NO-SPEC (regression guard, awaiting spec backfill).
// Pins NetID parser semantics: six dot-separated decimal octets → [6]byte.
func TestStringToNetID(t *testing.T) {
	tests := []struct {
		input    string
		expected [6]byte
	}{
		{"192.168.1.1.1.1", [6]byte{192, 168, 1, 1, 1, 1}},
		{"5.154.236.19.1.1", [6]byte{5, 154, 236, 19, 1, 1}},
		{"127.0.0.1.1.1", [6]byte{127, 0, 0, 1, 1, 1}},
		{"0.0.0.0.0.0", [6]byte{0, 0, 0, 0, 0, 0}},
		{"255.255.255.255.255.255", [6]byte{255, 255, 255, 255, 255, 255}},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result, err := ParseNetID(tt.input)
			if err != nil {
				t.Fatalf("ParseNetID(%q) unexpected error: %v", tt.input, err)
			}
			if result != tt.expected {
				t.Errorf("ParseNetID(%q) = %v, want %v", tt.input, result, tt.expected)
			}
		})
	}
}

// Validates: NO-SPEC (regression guard, awaiting spec backfill).
// Pins NetID parser error paths (wrong segment count, non-numeric, out-of-range).
func TestStringToNetIDErrors(t *testing.T) {
	tests := []struct {
		input string
		desc  string
	}{
		{"192.168.1.1", "too few parts"},
		{"192.168.1.1.1.1.1", "too many parts"},
		{"abc.168.1.1.1.1", "non-numeric part"},
		{"256.168.1.1.1.1", "value out of range"},
		{"", "empty string"},
	}
	for _, tt := range tests {
		t.Run(tt.desc, func(t *testing.T) {
			_, err := ParseNetID(tt.input)
			if err == nil {
				t.Errorf("ParseNetID(%q) expected error for %s, got nil", tt.input, tt.desc)
			}
		})
	}
}

// --- downgradeTransMode ---

// Validates: R-NOT-011.
func TestDowngradeTransMode(t *testing.T) {
	tests := []struct {
		input    TransMode
		expected TransMode
	}{
		{TransModeServerOnChange2, TransModeServerOnChange},
		{TransModeServerCycle2, TransModeServerCycle},
		{TransModeServerOnChange, TransModeServerOnChange},
		{TransModeServerCycle, TransModeServerCycle},
		{TransModeClientCycle, TransModeClientCycle},
		{TransModeNoTransmission, TransModeNoTransmission},
	}

	for _, tt := range tests {
		t.Run(tt.input.String(), func(t *testing.T) {
			result := downgradeTransMode(tt.input)
			if result != tt.expected {
				t.Errorf("downgradeTransMode(%v) = %v, want %v", tt.input, result, tt.expected)
			}
		})
	}
}

// --- TransMode.String ---

// Validates: NO-SPEC (regression guard, awaiting spec backfill).
// Pins TransMode Stringer output (ServerOnChange, ServerCycle2/CyclicInContext, Unknown(N)).
func TestTransModeString(t *testing.T) {
	if s := TransModeServerOnChange.String(); s != "ServerOnChange" {
		t.Errorf("got %q, want %q", s, "ServerOnChange")
	}
	if s := TransModeServerCycle2.String(); s != "ServerCycle2/CyclicInContext" {
		t.Errorf("got %q, want %q", s, "ServerCycle2/CyclicInContext")
	}
	if s := TransMode(99).String(); s != "Unknown(99)" {
		t.Errorf("got %q, want %q", s, "Unknown(99)")
	}
}

// --- SymbolFlag ---

// Validates: R-SYM-005.
func TestSymbolFlagContextMask(t *testing.T) {
	tests := []struct {
		flags   SymbolFlag
		ctxMask uint8
	}{
		{0x0008, 0},        // TypeGuid only, no context
		{0x0108, 1},        // ContextMask=1
		{0x0F08, 15},       // ContextMask=15 (max)
		{0x1008, 0},        // Attributes flag, no context
		{0x0308, 3},        // ContextMask=3
		{SymbolFlag(0), 0}, // No flags
		{0x8F08, 15},       // ExtendedFlags + max ContextMask
	}
	for _, tt := range tests {
		got := tt.flags.ContextMask()
		if got != tt.ctxMask {
			t.Errorf("SymbolFlag(0x%04X).ContextMask() = %d, want %d", uint32(tt.flags), got, tt.ctxMask)
		}
	}
}

// Validates: NO-SPEC (regression guard, awaiting spec backfill).
// Pins SymbolFlag.Has helper bit-test against TypeGuid / Attributes / ExtendedFlags bits.
func TestSymbolFlagHas(t *testing.T) {
	f := SymbolFlag(0x1008) // TypeGuid + Attributes
	if !f.Has(SymbolFlagTypeGuid) {
		t.Error("expected Has(TypeGuid) = true")
	}
	if !f.Has(SymbolFlagAttributes) {
		t.Error("expected Has(Attributes) = true")
	}
	if f.Has(SymbolFlagExtendedFlags) {
		t.Error("expected Has(ExtendedFlags) = false")
	}
}

// Validates: NO-SPEC (regression guard, awaiting spec backfill).
// Pins SymbolFlagBitValue detection (set, unset, combined with other flags).
func TestSymbolFlagBitValue_Detection(t *testing.T) {
	flags := SymbolFlag(0x0002)
	if !flags.Has(SymbolFlagBitValue) {
		t.Error("expected Has(SymbolFlagBitValue) to be true")
	}
	flags = SymbolFlag(0x0000)
	if flags.Has(SymbolFlagBitValue) {
		t.Error("expected Has(SymbolFlagBitValue) to be false for 0x0000")
	}
	// Combined flags
	flags = SymbolFlag(0x0F03) // Persistent | BitValue | ContextMask
	if !flags.Has(SymbolFlagBitValue) {
		t.Error("expected Has(SymbolFlagBitValue) to be true for combined flags")
	}
}

// --- ReturnCode.String ---

// Validates: NO-SPEC (regression guard, awaiting spec backfill).
// Pins ReturnCode Stringer messages for representative no-error/global/device/client codes.
func TestReturnCodeString(t *testing.T) {
	tests := []struct {
		code     ReturnCode
		contains string
	}{
		{ReturnCodeNoErrors, "no error"},
		{ReturnCodeGlobalTargetNotFound, "target machine not found"},
		{ReturnCodeDeviceSymbolNoFound, "symbol not found"},
		{ReturnCodeClientSyncTimeout, "timeout elapsed"},
		{ReturnCode(0xFFFF), "unknown error code"},
	}

	for _, tt := range tests {
		t.Run(tt.contains, func(t *testing.T) {
			s := tt.code.String()
			if !strings.Contains(s, tt.contains) {
				t.Errorf("ReturnCode(0x%04X).String() = %q, want it to contain %q", uint32(tt.code), s, tt.contains)
			}
		})
	}
}

// Validates: NO-SPEC (regression guard, awaiting spec backfill).
// Pins ReturnCode satisfying the error interface with a non-empty Error() string.
func TestReturnCodeError(t *testing.T) {
	rc := ReturnCodeDeviceBusy
	var err error = rc
	if err.Error() == "" {
		t.Error("expected non-empty error string")
	}
}

// --- buildTag ---

// Validates: R-ROUTE-001.
func TestBuildTag(t *testing.T) {
	tag := buildTag(7, []byte{192, 168, 1, 1, 1, 1})
	if len(tag) != 10 {
		t.Fatalf("tag length = %d, want 10", len(tag))
	}
	tid := binary.LittleEndian.Uint16(tag[0:])
	if tid != 7 {
		t.Errorf("tag ID = %d, want 7", tid)
	}
	tlen := binary.LittleEndian.Uint16(tag[2:])
	if tlen != 6 {
		t.Errorf("tag length field = %d, want 6", tlen)
	}
}

// --- appendNull ---

// Validates: NO-SPEC (regression guard, awaiting spec backfill).
// Pins null-terminator append helper used by route tag builders.
func TestAppendNull(t *testing.T) {
	result := appendNull([]byte("hello"))
	if len(result) != 6 {
		t.Fatalf("length = %d, want 6", len(result))
	}
	if result[5] != 0 {
		t.Error("last byte should be null terminator")
	}
}

// --- ADST_ type code tests ---

// Validates: R-SYM-004.
func TestADSTypeToString(t *testing.T) {
	tests := []struct {
		code ADSDataType
		want string
	}{
		{ADSTBool, "BOOL"},
		{ADSTInt8, "SINT"},
		{ADSTUint8, "USINT"},
		{ADSTInt16, "INT"},
		{ADSTUint16, "UINT"},
		{ADSTInt32, "DINT"},
		{ADSTUint32, "UDINT"},
		{ADSTReal32, "REAL"},
		{ADSTReal64, "LREAL"},
		{ADSTInt64, "LINT"},
		{ADSTUint64, "ULINT"},
		{ADSTString, "STRING"},
		{ADSTWString, "WSTRING"},
		{ADSTVoid, ""},
		{999, ""},
	}
	for _, tt := range tests {
		got := adsTypeToString(tt.code)
		if got != tt.want {
			t.Errorf("adsTypeToString(%d) = %q, want %q", tt.code, got, tt.want)
		}
	}
}

// Validates: R-NOT-016.
func TestUpdate_StaleReasonFields(t *testing.T) {
	u := Update{Variable: "x", Value: "1", Stale: &StaleInfo{Reason: ReasonSymbolVersionInvalid}}
	if u.Stale == nil {
		t.Error("Stale field missing")
	}
	if u.Stale.Reason != ReasonSymbolVersionInvalid {
		t.Errorf("Stale.Reason field missing or wrong: %q", u.Stale.Reason)
	}
}

// Validates: R-SES-011.
func TestSymbolVersionStrategy_String(t *testing.T) {
	tests := []struct {
		s    SymbolVersionStrategy
		want string
	}{
		{SymbolVersionAutoReload, "AutoReload"},
		{SymbolVersionClose, "Close"},
		{SymbolVersionIgnore, "Ignore"},
	}
	for _, tt := range tests {
		if got := tt.s.String(); got != tt.want {
			t.Errorf("strategy %d → %q, want %q", tt.s, got, tt.want)
		}
	}
}

// Validates: R-NOT-016 reason enumeration.
func TestStaleReasonConstants(t *testing.T) {
	cases := map[Reason]string{
		ReasonSymbolVersionInvalid: "symbol-version-invalid",
		ReasonSymbolNotFound:       "symbol-not-found",
		ReasonInvalidOffset:        "invalid-offset",
		ReasonSymbolNotActive:      "symbol-not-active",
		ReasonNotifyHandleInvalid:  "notify-handle-invalid",
		ReasonInvalidSize:          "invalid-size",
		ReasonReloadCapExhausted:   "reload-cap-exhausted",
		ReasonReloadInProgress:     "reload-in-progress",
	}
	for got, want := range cases {
		if string(got) != want {
			t.Errorf("constant value drift: got %q, want %q", string(got), want)
		}
	}
}

// Validates: R-CACHE-009 detection set + R-NOT-016 reason mapping.
func TestDetectStaleCache(t *testing.T) {
	tests := []struct {
		rc        ReturnCode
		wantStale bool
		wantReas  Reason
	}{
		// 5 codes in detection set.
		{ReturnCodeDeviceSymbolVersionInvalid, true, ReasonSymbolVersionInvalid}, // 0x711
		{ReturnCodeDeviceSymbolNoFound, true, ReasonSymbolNotFound},              // 0x710
		{ReturnCodeDeviceInvalidOffset, true, ReasonInvalidOffset},               // 0x703
		{ReturnCodeDeviceSymbolNotActive, true, ReasonSymbolNotActive},           // 0x722
		{ReturnCodeDeviceNotifyHandleInvalid, true, ReasonNotifyHandleInvalid},   // 0x714
		{ReturnCodeDeviceInvalidSize, true, ReasonInvalidSize},                   // 0x705
		// Negative cases — must NOT trigger.
		{ReturnCodeNoErrors, false, ""},
		{ReturnCodeDeviceTimeout, false, ""},
		{ReturnCodeDeviceWarning, false, ""}, // 0x720 — explicitly NOT in set (Beckhoff: signal warning)
	}
	for _, tt := range tests {
		stale, reason := detectStaleCache(tt.rc)
		if stale != tt.wantStale || reason != tt.wantReas {
			t.Errorf("detectStaleCache(0x%X) = (%v, %q), want (%v, %q)",
				uint32(tt.rc), stale, reason, tt.wantStale, tt.wantReas)
		}
	}
}
