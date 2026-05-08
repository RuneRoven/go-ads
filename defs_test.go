package ads

import (
	"encoding/binary"
	"strings"
	"testing"
)

// --- stringToNetID ---

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
			result, err := stringToNetID(tt.input)
			if err != nil {
				t.Fatalf("stringToNetID(%q) unexpected error: %v", tt.input, err)
			}
			if result != tt.expected {
				t.Errorf("stringToNetID(%q) = %v, want %v", tt.input, result, tt.expected)
			}
		})
	}
}

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
			_, err := stringToNetID(tt.input)
			if err == nil {
				t.Errorf("stringToNetID(%q) expected error for %s, got nil", tt.input, tt.desc)
			}
		})
	}
}

// --- downgradeTransMode ---

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

func TestReturnCodeError(t *testing.T) {
	rc := ReturnCodeDeviceBusy
	var err error = rc
	if err.Error() == "" {
		t.Error("expected non-empty error string")
	}
}

func TestReturnCodeString_AllCategories(t *testing.T) {
	tests := []struct {
		code     ReturnCode
		contains string
	}{
		// Global errors
		{ReturnCodeGlobalInternalError, "internal error"},
		{ReturnCodeGlobalTargetPortNotFound, "target port not found"},
		{ReturnCodeGlobalInvalidAdsLength, "invalid ADS length"},
		{ReturnCodeGlobalInvalidAmsNetID, "invalid AMS Net ID"},
		{ReturnCodeGlobalTcpSendError, "TCP send error"},
		{ReturnCodeGlobalHostUnreachable, "host unreachable"},
		{ReturnCodeGlobalAccessDenied, "access denied"},

		// Router errors
		{ReturnCodeRouterNoLockedMemory, "locked memory"},
		{ReturnCodeRouterMailboxFull, "mailbox full"},
		{ReturnCodeRouterNotInitialized, "not initialized"},
		{ReturnCodeRouterPortAlreadyInUse, "already assigned"},

		// Device errors
		{ReturnCodeDeviceError, "general device error"},
		{ReturnCodeDeviceServiceNotSupported, "service not supported"},
		{ReturnCodeDeviceInvalidGroup, "invalid index group"},
		{ReturnCodeDeviceInvalidOffset, "invalid index offset"},
		{ReturnCodeDeviceInvalidSize, "parameter size not correct"},
		{ReturnCodeDeviceInvalidData, "invalid parameter value"},
		{ReturnCodeDeviceNotReady, "not in a ready state"},
		{ReturnCodeDeviceInvalidContext, "invalid operating system context"},
		{ReturnCodeDeviceInvalidParam, "invalid parameter value"},
		{ReturnCodeDeviceTimeout, "timeout"},
		{ReturnCodeDeviceTransModeNotSupported, "TransMode not supported"},
		{ReturnCodeDeviceNotifyHandleInvalid, "notification handle is invalid"},
		{ReturnCodeDeviceNoMoreHandles, "no more notification handles"},
		{ReturnCodeDeviceInvalidWatchSize, "notification size too large"},
		{ReturnCodeDeviceInvalidArrayIndex, "invalid array index"},
		{ReturnCodeDeviceSymbolNotActive, "symbol not active"},
		{ReturnCodeDeviceAccessDenied, "access denied"},
		{ReturnCodeDeviceLicenseNotFound, "missing license"},
		{ReturnCodeDeviceLicenseExpired, "license expired"},

		// Client errors
		{ReturnCodeClientError, "client error"},
		{ReturnCodeClientInvalidParameter, "invalid parameter"},
		{ReturnCodeClientSyncTimeout, "timeout elapsed"},
		{ReturnCodeClientPortNotOpen, "port not opened"},
		{ReturnCodeClientRequestCancelled, "cancelled"},

		// RTime errors
		{ReturnCodeRTimeInternal, "fatal error"},
		{ReturnCodeRTimeBadTimerPeriods, "timer value not valid"},

		// TCP errors
		{ReturnCodeWsaeTimedOut, "timed out"},
		{ReturnCodeWsaeConnRefused, "refused"},
		{ReturnCodeWsaeHostDown, "host is down"},
	}

	for _, tt := range tests {
		t.Run(tt.contains, func(t *testing.T) {
			s := tt.code.String()
			if !strings.Contains(strings.ToLower(s), strings.ToLower(tt.contains)) {
				t.Errorf("ReturnCode(0x%04X).String() = %q, want it to contain %q",
					uint32(tt.code), s, tt.contains)
			}
		})
	}
}

func TestReturnCodeError_ImplementsError(t *testing.T) {
	codes := []ReturnCode{
		ReturnCodeDeviceTimeout,
		ReturnCodeDeviceInvalidParam,
		ReturnCodeDeviceSymbolNoFound,
		ReturnCodeClientSyncTimeout,
		ReturnCodeGlobalTargetNotFound,
	}
	for _, rc := range codes {
		var err error = rc
		if err.Error() == "" {
			t.Errorf("ReturnCode(0x%04X).Error() should not be empty", uint32(rc))
		}
	}
}

// --- buildTag ---

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

func TestAppendNull(t *testing.T) {
	result := appendNull([]byte("hello"))
	if len(result) != 6 {
		t.Fatalf("length = %d, want 6", len(result))
	}
	if result[5] != 0 {
		t.Error("last byte should be null terminator")
	}
}

// ==========================================================================
// Process image constants and helpers
// ==========================================================================

func TestProcessImageConstants(t *testing.T) {
	tests := []struct {
		name string
		got  Group
		want uint32
	}{
		{"GroupIoImageRwib", GroupIoImageRwib, 0xF020},
		{"GroupIoImageRwix", GroupIoImageRwix, 0xF021},
		{"GroupIoImageRisize", GroupIoImageRisize, 0xF025},
		{"GroupIoImageRwob", GroupIoImageRwob, 0xF030},
		{"GroupIoImageRwox", GroupIoImageRwox, 0xF031},
		{"GroupIoImageCleari", GroupIoImageCleari, 0xF040},
		{"GroupIoImageClearo", GroupIoImageClearo, 0xF050},
		{"GroupIoImageRwiob", GroupIoImageRwiob, 0xF060},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if uint32(tt.got) != tt.want {
				t.Errorf("%s = 0x%04X, want 0x%04X", tt.name, uint32(tt.got), tt.want)
			}
		})
	}
}

func TestProcessImageBitOffset(t *testing.T) {
	// Verify the bit offset calculation used in process image bit access
	// byteOffset * 8 + bitIndex
	tests := []struct {
		byteOffset uint32
		bitIndex   uint8
		want       uint32
	}{
		{0, 0, 0},
		{0, 7, 7},
		{1, 0, 8},
		{1, 3, 11},
		{10, 5, 85},
	}
	for _, tt := range tests {
		got := tt.byteOffset*8 + uint32(tt.bitIndex)
		if got != tt.want {
			t.Errorf("byte=%d bit=%d: got %d, want %d", tt.byteOffset, tt.bitIndex, got, tt.want)
		}
	}
}

// --- ADST_ type code tests ---

func TestADSTypeToString(t *testing.T) {
	tests := []struct {
		code uint32
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
