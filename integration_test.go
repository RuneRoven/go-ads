//go:build integration

package ads

import (
	"encoding/binary"
	"math"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"
)

// Integration tests require a real Beckhoff PLC.
// Run with: go test -tags integration -v -timeout 30s ./...
//
// Environment variables:
//   ADS_PLC_IP       - PLC IP address (default: 192.168.3.224)
//   ADS_TARGET_AMS   - PLC AMS NetID (default: 5.154.236.19.1.1)
//   ADS_TARGET_PORT  - PLC AMS port (default: 851)
//   ADS_LOCAL_AMS    - Local AMS NetID (default: auto-derived from local IP)
//   ADS_HOST_IP      - Local IP the PLC should use to reach us (default: auto-derived)
//   ADS_SYMBOL_NAME  - Symbol to read (default: first found)
//   ADS_ROUTE_USER   - PLC admin username for auto-creating AMS route (optional)
//   ADS_ROUTE_PASS   - PLC admin password for auto-creating AMS route (optional)

func getEnvOrDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// parseableSet contains data types that parse() can handle without full datatype resolution.
var parseableSet = map[string]bool{
	"BOOL": true, "BYTE": true, "USINT": true, "SINT": true,
	"UINT": true, "UINT16": true, "WORD": true, "INT": true, "INT16": true,
	"UDINT": true, "DWORD": true, "DINT": true,
	"REAL": true, "LREAL": true, "STRING": true,
	"LINT": true, "ULINT": true, "LWORD": true,
	"TIME": true, "TOD": true, "TIME_OF_DAY": true,
	"DATE": true, "DT": true, "DATE_AND_TIME": true,
}

// pickParseableSymbol returns the name of a symbol with a parseable base type from the map.
// Returns "" if none found.
func pickParseableSymbol(symbols map[string]SymbolView) string {
	for name, sym := range symbols {
		if parseableSet[sym.DataType] {
			return name
		}
	}
	return ""
}

// pickParseableSymbols returns up to n symbol names with parseable base types.
// Prefers top-level symbols (no dots) to avoid struct children that may lack handles.
func pickParseableSymbols(symbols map[string]SymbolView, n int) []string {
	var topLevel, nested []string
	for name, sym := range symbols {
		if !parseableSet[sym.DataType] {
			continue
		}
		if !strings.Contains(name, ".") {
			topLevel = append(topLevel, name)
		} else {
			nested = append(nested, name)
		}
	}
	var names []string
	for _, name := range topLevel {
		names = append(names, name)
		if len(names) >= n {
			return names
		}
	}
	for _, name := range nested {
		names = append(names, name)
		if len(names) >= n {
			return names
		}
	}
	return names
}

func setupConnection(t *testing.T) *Connection {
	t.Helper()
	return setupConnectionWithDefaults(t, connDefaults{
		ip:        "192.168.3.224",
		targetAMS: "5.154.236.19.1.1",
		routeName: "go-ads-test",
	})
}

func TestIntegrationConnect(t *testing.T) {
	conn := setupConnection(t)
	if conn.IsDisconnected() {
		t.Error("expected connected state")
	}
}

func TestIntegrationReadDeviceInfo(t *testing.T) {
	conn := setupConnection(t)
	info, err := conn.ReadDeviceInfo()
	if err != nil {
		t.Fatalf("ReadDeviceInfo failed: %v", err)
	}
	t.Logf("Device: %s (version %d.%d.%d)", info.DeviceName, info.Major, info.Minor, info.Version)
}

func TestIntegrationReadState(t *testing.T) {
	conn := setupConnection(t)
	state, err := conn.ReadState()
	if err != nil {
		t.Fatalf("ReadState failed: %v", err)
	}
	t.Logf("ADS state: %d, Device state: %d", state.ADSState, state.DeviceState)
	if state.ADSState != ADSStateRun {
		t.Logf("warning: PLC not in Run state (got %d)", state.ADSState)
	}
}

func TestIntegrationListSymbols(t *testing.T) {
	conn := setupConnection(t)

	// ListSymbols should fail before LoadSymbols
	_, err := conn.ListSymbols()
	if err == nil {
		t.Error("expected error from ListSymbols before LoadSymbols")
	}

	err = conn.LoadSymbols()
	if err != nil {
		t.Fatalf("LoadSymbols failed: %v", err)
	}

	symbols, err := conn.ListSymbols()
	if err != nil {
		t.Fatalf("ListSymbols failed: %v", err)
	}
	if len(symbols) == 0 {
		t.Fatal("expected at least one symbol")
	}
	t.Logf("found %d symbols", len(symbols))
	count := 0
	for name, sym := range symbols {
		if count < 5 {
			t.Logf("  %s (%s, length=%d)", name, sym.DataType, sym.Length)
			count++
		}
	}
}

func TestIntegrationReadStructWithEnum(t *testing.T) {
	conn := setupConnection(t)

	if err := conn.LoadSymbols(); err != nil {
		t.Fatalf("LoadSymbols failed: %v", err)
	}

	symbols, err := conn.ListSymbols()
	if err != nil {
		t.Fatalf("ListSymbols failed: %v", err)
	}

	structName := os.Getenv("ADS_READ_STRUCT")
	var hasEnumChild bool

	if structName == "" {
		// Auto-discover: prefer struct with enum/alias child
		for name, sym := range symbols {
			if len(sym.Children()) == 0 || !sym.IsRoot {
				continue
			}
			for _, child := range sym.Children() {
				if !parseableSet[child.DataType] {
					hasEnumChild = true
					break
				}
			}
			if hasEnumChild {
				structName = name
				break
			}
		}
	}

	if structName == "" {
		// Fallback: any struct symbol
		for name, sym := range symbols {
			if len(sym.Children()) > 0 && sym.IsRoot {
				structName = name
				break
			}
		}
	}

	if structName == "" {
		t.Skip("no struct symbols found on PLC (set ADS_READ_STRUCT)")
	}

	sym, ok := symbols[structName]
	if !ok {
		t.Skipf("struct symbol %q not found in symbol table (PLC may not have this variable)", structName)
	}
	// Check if chosen symbol has enum/alias children
	if !hasEnumChild {
		for _, child := range sym.Children() {
			if !parseableSet[child.DataType] {
				hasEnumChild = true
				break
			}
		}
	}

	t.Logf("testing struct %s (type=%s, children=%d, hasEnumChild=%v)",
		structName, sym.DataType, len(sym.Children()), hasEnumChild)

	// Dump datatype table info for each child, especially enum/alias types
	conn.cache.lock.Lock()
	datatypes := conn.cache.datatypes
	conn.cache.lock.Unlock()
	for childName, child := range sym.Children() {
		t.Logf("  child %s (type=%s, length=%d)", childName, child.DataType, child.Length)
		if !parseableSet[child.DataType] && datatypes != nil {
			if dt, ok := datatypes[child.DataType]; ok {
				t.Logf("    datatype %q: baseType=%q, size=%d, arrayDim=%d, subItems=%d, children=%d",
					dt.Name, dt.DataType, dt.DatatypeEntry.Size, dt.DatatypeEntry.ArrayDim,
					dt.DatatypeEntry.SubItems, len(dt.Children))
			} else {
				t.Logf("    datatype %q: NOT FOUND in datatype table", child.DataType)
			}
		}
	}

	value, err := conn.ReadFromSymbol(structName)
	if err != nil {
		t.Fatalf("ReadFromSymbol(%q) failed: %v", structName, err)
	}
	t.Logf("%s = %s", structName, value)

	// Every child must have a parsed value after reading the struct
	for childName, child := range sym.Children() {
		// F-31: empty Value is valid for empty STRING/WSTRING leaves and for
		// non-leaf nodes (their value is computed from children via GetJSON,
		// which may produce an empty/{} string).
		if len(child.Children()) == 0 && child.Value == "" && child.DataType != "STRING" && child.DataType != "WSTRING" {
			t.Errorf("child %q has empty Value after struct read", childName)
		}
	}
}

func TestIntegrationReadSymbol(t *testing.T) {
	conn := setupConnection(t)
	symbolName := os.Getenv("ADS_SYMBOL_NAME")
	if symbolName == "" {
		// Need full discovery to pick a symbol
		err := conn.LoadSymbols()
		if err != nil {
			t.Fatalf("LoadSymbols failed: %v", err)
		}
		symbols, _ := conn.ListSymbols()
		symbolName = pickParseableSymbol(symbols)
	}
	if symbolName == "" {
		t.Skip("no parseable leaf symbols available")
	}

	// ReadFromSymbol uses on-demand resolution if symbol not already loaded
	value, err := conn.ReadFromSymbol(symbolName)
	if err != nil {
		t.Fatalf("ReadFromSymbol(%q) failed: %v", symbolName, err)
	}
	t.Logf("%s = %s", symbolName, value)
}

func TestIntegrationBrowseSymbols(t *testing.T) {
	conn := setupConnection(t)

	// BrowseSymbols should fail before LoadSymbolList
	_, err := conn.BrowseSymbols("")
	if err == nil {
		t.Error("expected error from BrowseSymbols before LoadSymbolList")
	}

	err = conn.LoadSymbolList(SlowDiscoveryConfig{})
	if err != nil {
		t.Fatalf("LoadSymbolList failed: %v", err)
	}

	// Browse root
	entries, err := conn.BrowseSymbols("")
	if err != nil {
		t.Fatalf("BrowseSymbols('') failed: %v", err)
	}
	if len(entries) == 0 {
		t.Fatal("expected at least one root entry")
	}
	t.Logf("root entries: %d", len(entries))
	for _, e := range entries {
		t.Logf("  %s (type=%s, size=%d, hasChildren=%v)", e.FullName, e.DataType, e.Size, e.HasChildren)
	}

	// Browse into the first root entry that has children
	for _, e := range entries {
		if e.HasChildren {
			children, err := conn.BrowseSymbols(e.FullName)
			if err != nil {
				t.Fatalf("BrowseSymbols(%q) failed: %v", e.FullName, err)
			}
			t.Logf("children of %s: %d", e.FullName, len(children))
			for i, c := range children {
				if i < 5 {
					t.Logf("  %s (type=%s, size=%d, hasChildren=%v)", c.FullName, c.DataType, c.Size, c.HasChildren)
				}
			}
			break
		}
	}
}

func TestIntegrationBrowseWithDataTypes(t *testing.T) {
	conn := setupConnection(t)

	// Load symbols first, then datatypes
	err := conn.LoadSymbolList(SlowDiscoveryConfig{})
	if err != nil {
		t.Fatalf("LoadSymbolList failed: %v", err)
	}

	err = conn.LoadDataTypes(SlowDiscoveryConfig{})
	if err != nil {
		t.Fatalf("LoadDataTypes failed: %v", err)
	}

	// Browse root
	entries, err := conn.BrowseSymbols("")
	if err != nil {
		t.Fatalf("BrowseSymbols('') failed: %v", err)
	}
	t.Logf("root entries after full browse: %d", len(entries))

	// Try to find a struct symbol and browse into it
	for _, e := range entries {
		if e.HasChildren {
			children, err := conn.BrowseSymbols(e.FullName)
			if err != nil {
				t.Fatalf("BrowseSymbols(%q) failed: %v", e.FullName, err)
			}
			t.Logf("children of %s (with datatypes): %d", e.FullName, len(children))
			for i, c := range children {
				if i < 10 {
					t.Logf("  %s (type=%s, size=%d, hasChildren=%v)", c.FullName, c.DataType, c.Size, c.HasChildren)
				}
			}
			// Try one more level deep if any child has children
			for _, c := range children {
				if c.HasChildren {
					grandchildren, err := conn.BrowseSymbols(c.FullName)
					if err != nil {
						t.Logf("BrowseSymbols(%q) failed: %v", c.FullName, err)
						continue
					}
					t.Logf("grandchildren of %s: %d", c.FullName, len(grandchildren))
					for i, gc := range grandchildren {
						if i < 5 {
							t.Logf("    %s (type=%s)", gc.FullName, gc.DataType)
						}
					}
					break
				}
			}
			break
		}
	}
}

func TestIntegrationNotification(t *testing.T) {
	conn := setupConnection(t)
	symbolName := os.Getenv("ADS_SYMBOL_NAME")
	if symbolName == "" {
		err := conn.LoadSymbols()
		if err != nil {
			t.Fatalf("LoadSymbols failed: %v", err)
		}
		symbols, _ := conn.ListSymbols()
		symbolName = pickParseableSymbol(symbols)
	}
	if symbolName == "" {
		t.Skip("no parseable symbols available")
	}

	// Verify no active notifications before subscribe
	conn.notifs.lock.Lock()
	beforeCount := len(conn.notifs.activeNotifications)
	conn.notifs.lock.Unlock()
	if beforeCount != 0 {
		t.Fatalf("expected 0 active notifications before subscribe, got %d", beforeCount)
	}

	ch := make(chan *Update, 10)
	handle, err := conn.AddSymbolNotification(symbolName, 100*time.Millisecond, 100*time.Millisecond, TransModeServerOnChange, ch)
	if err != nil {
		t.Fatalf("AddSymbolNotification(%q) failed: %v", symbolName, err)
	}
	t.Logf("notification handle: %d", handle)

	// Verify handle is tracked
	conn.notifs.lock.Lock()
	afterCount := len(conn.notifs.activeNotifications)
	_, tracked := conn.notifs.activeNotifications[handle]
	conn.notifs.lock.Unlock()
	if afterCount != 1 {
		t.Errorf("expected 1 active notification after subscribe, got %d", afterCount)
	}
	if !tracked {
		t.Errorf("handle %d not found in activeNotifications", handle)
	}

	select {
	case update := <-ch:
		t.Logf("notification: %s = %s at %v", update.Variable, update.Value, update.TimeStamp)
	case <-time.After(5 * time.Second):
		t.Log("no notification received within 5s (may be expected if value doesn't change)")
	}

	// Clean up: delete the notification and verify handle removal
	err = conn.DeleteDeviceNotification(handle)
	if err != nil {
		t.Fatalf("DeleteDeviceNotification(%d) failed: %v", handle, err)
	}
	conn.notifs.lock.Lock()
	cleanupCount := len(conn.notifs.activeNotifications)
	conn.notifs.lock.Unlock()
	if cleanupCount != 0 {
		t.Errorf("expected 0 active notifications after delete, got %d", cleanupCount)
	}
	t.Logf("handle %d cleaned up successfully", handle)
}

func TestIntegrationSubscribeUnsubscribe(t *testing.T) {
	conn := setupConnection(t)
	symbolName := os.Getenv("ADS_SYMBOL_NAME")
	if symbolName == "" {
		err := conn.LoadSymbols()
		if err != nil {
			t.Fatalf("LoadSymbols failed: %v", err)
		}
		symbols, _ := conn.ListSymbols()
		symbolName = pickParseableSymbol(symbols)
	}
	if symbolName == "" {
		t.Skip("no parseable symbols available")
	}

	ch := make(chan *Update, 10)

	// Verify clean state
	conn.notifs.lock.Lock()
	if len(conn.notifs.activeNotifications) != 0 {
		t.Fatalf("expected 0 active notifications at start, got %d", len(conn.notifs.activeNotifications))
	}
	conn.notifs.lock.Unlock()

	// Subscribe
	handle, err := conn.AddSymbolNotification(symbolName, 100*time.Millisecond, 100*time.Millisecond, TransModeServerOnChange, ch)
	if err != nil {
		t.Fatalf("AddSymbolNotification(%q) failed: %v", symbolName, err)
	}
	t.Logf("subscribed to %s (handle=%d)", symbolName, handle)

	// Verify handle is tracked
	conn.notifs.lock.Lock()
	if _, ok := conn.notifs.activeNotifications[handle]; !ok {
		t.Errorf("handle %d not tracked in activeNotifications after subscribe", handle)
	}
	conn.notifs.lock.Unlock()

	// Wait briefly for a notification
	select {
	case update := <-ch:
		t.Logf("notification before unsubscribe: %s = %s", update.Variable, update.Value)
	case <-time.After(2 * time.Second):
		t.Log("no notification within 2s (continuing to unsubscribe)")
	}

	// Unsubscribe
	err = conn.DeleteDeviceNotification(handle)
	if err != nil {
		t.Fatalf("DeleteDeviceNotification(%d) failed: %v", handle, err)
	}
	t.Logf("unsubscribed handle %d", handle)

	// Verify handle removed from tracking
	conn.notifs.lock.Lock()
	if _, ok := conn.notifs.activeNotifications[handle]; ok {
		t.Errorf("handle %d still in activeNotifications after DeleteDeviceNotification", handle)
	}
	if len(conn.notifs.activeNotifications) != 0 {
		t.Errorf("expected 0 active notifications after unsubscribe, got %d", len(conn.notifs.activeNotifications))
	}
	conn.notifs.lock.Unlock()

	// Verify no more notifications arrive after unsubscribe
	select {
	case update := <-ch:
		t.Logf("warning: received notification after unsubscribe: %s = %s (may be in-flight)", update.Variable, update.Value)
	case <-time.After(2 * time.Second):
		t.Log("no notification after unsubscribe (expected)")
	}
}

func TestIntegrationHandleLeakMultipleSubscriptions(t *testing.T) {
	conn := setupConnection(t)

	err := conn.LoadSymbols()
	if err != nil {
		t.Fatalf("LoadSymbols failed: %v", err)
	}

	// Collect up to 3 distinct parseable symbol names to subscribe to
	symbols, _ := conn.ListSymbols()
	symbolNames := pickParseableSymbols(symbols, 3)
	if len(symbolNames) == 0 {
		t.Skip("no parseable symbols available")
	}

	ch := make(chan *Update, 100)

	// Verify clean state
	conn.notifs.lock.Lock()
	startCount := len(conn.notifs.activeNotifications)
	conn.notifs.lock.Unlock()
	if startCount != 0 {
		t.Fatalf("expected 0 active notifications at start, got %d", startCount)
	}

	// Subscribe to multiple symbols
	var handles []uint32
	for _, name := range symbolNames {
		handle, err := conn.AddSymbolNotification(name, 100*time.Millisecond, 100*time.Millisecond, TransModeServerOnChange, ch)
		if err != nil {
			t.Fatalf("AddSymbolNotification(%q) failed: %v", name, err)
		}
		handles = append(handles, handle)
		t.Logf("subscribed to %s (handle=%d)", name, handle)
	}

	// Verify all handles are tracked
	conn.notifs.lock.Lock()
	activeCount := len(conn.notifs.activeNotifications)
	conn.notifs.lock.Unlock()
	if activeCount != len(handles) {
		t.Errorf("expected %d active notifications, got %d", len(handles), activeCount)
	}

	// Delete handles one by one and verify count decreases
	for i, handle := range handles {
		err := conn.DeleteDeviceNotification(handle)
		if err != nil {
			t.Fatalf("DeleteDeviceNotification(%d) failed: %v", handle, err)
		}
		conn.notifs.lock.Lock()
		remaining := len(conn.notifs.activeNotifications)
		conn.notifs.lock.Unlock()
		expected := len(handles) - i - 1
		if remaining != expected {
			t.Errorf("after deleting handle %d: expected %d active, got %d", handle, expected, remaining)
		}
	}

	// Final check: zero handles remaining
	conn.notifs.lock.Lock()
	finalCount := len(conn.notifs.activeNotifications)
	conn.notifs.lock.Unlock()
	if finalCount != 0 {
		t.Errorf("expected 0 active notifications after deleting all, got %d", finalCount)
	}
	t.Logf("all %d handles cleaned up successfully", len(handles))
}

func TestIntegrationCloseReleasesNotificationHandles(t *testing.T) {
	// This test verifies that Close() properly releases notification handles
	// on the PLC, preventing handle leaks that fill up the PLC.
	ip := getEnvOrDefault("ADS_PLC_IP", "192.168.3.224")
	targetAMS := getEnvOrDefault("ADS_TARGET_AMS", "5.154.236.19.1.1")
	targetPortStr := getEnvOrDefault("ADS_TARGET_PORT", "851")
	targetPort, err := strconv.Atoi(targetPortStr)
	if err != nil {
		t.Fatalf("invalid ADS_TARGET_PORT %q: %v", targetPortStr, err)
	}
	localAMS := getEnvOrDefault("ADS_LOCAL_AMS", "auto")

	conn, err := NewConnection(ip, 48898, targetAMS, targetPort, localAMS, 10500, 5*time.Second)
	if err != nil {
		t.Fatalf("NewConnection failed: %v", err)
	}
	err = conn.Connect(false)
	if err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	// Do NOT use t.Cleanup(conn.Close) — we call Close() explicitly to test it

	err = conn.LoadSymbols()
	if err != nil {
		conn.Close()
		t.Fatalf("LoadSymbols failed: %v", err)
	}

	symbols, _ := conn.ListSymbols()
	symbolNames := pickParseableSymbols(symbols, 3)
	if len(symbolNames) == 0 {
		conn.Close()
		t.Skip("no parseable symbols available")
	}

	ch := make(chan *Update, 100)
	var handles []uint32
	for _, name := range symbolNames {
		handle, err := conn.AddSymbolNotification(name, 100*time.Millisecond, 100*time.Millisecond, TransModeServerOnChange, ch)
		if err != nil {
			conn.Close()
			t.Fatalf("AddSymbolNotification(%q) failed: %v", name, err)
		}
		handles = append(handles, handle)
		t.Logf("subscribed to %s (handle=%d)", name, handle)
	}

	// Verify handles are active
	conn.notifs.lock.Lock()
	activeBeforeClose := len(conn.notifs.activeNotifications)
	conn.notifs.lock.Unlock()
	if activeBeforeClose != len(handles) {
		t.Errorf("expected %d active notifications before Close, got %d", len(handles), activeBeforeClose)
	}

	// Close should release all handles on the PLC
	conn.Close()
	t.Logf("Close() completed, released %d notification handles", len(handles))

	// Reconnect and verify no stale handles exist by subscribing to the same
	// symbols again — if Close() didn't release, the PLC would eventually
	// run out of handles.
	conn2, err := NewConnection(ip, 48898, targetAMS, targetPort, localAMS, 10501, 5*time.Second)
	if err != nil {
		t.Fatalf("second NewConnection failed: %v", err)
	}
	err = conn2.Connect(false)
	if err != nil {
		t.Fatalf("second Connect failed: %v", err)
	}
	defer conn2.Close()

	err = conn2.LoadSymbols()
	if err != nil {
		t.Fatalf("second LoadSymbols failed: %v", err)
	}

	// If old handles leaked, subscribing again would still work (PLC allows many),
	// but we verify no tracking leaks on our side
	conn2.notifs.lock.Lock()
	freshCount := len(conn2.notifs.activeNotifications)
	conn2.notifs.lock.Unlock()
	if freshCount != 0 {
		t.Errorf("fresh connection should have 0 active notifications, got %d", freshCount)
	}

	// Subscribe to same symbols on new connection to confirm PLC accepts them
	ch2 := make(chan *Update, 100)
	for _, name := range symbolNames {
		handle, err := conn2.AddSymbolNotification(name, 100*time.Millisecond, 100*time.Millisecond, TransModeServerOnChange, ch2)
		if err != nil {
			t.Errorf("re-subscribe to %s on fresh connection failed: %v (possible PLC handle leak)", name, err)
		} else {
			t.Logf("re-subscribed to %s (new handle=%d)", name, handle)
		}
	}

	conn2.notifs.lock.Lock()
	resubCount := len(conn2.notifs.activeNotifications)
	conn2.notifs.lock.Unlock()
	if resubCount != len(symbolNames) {
		t.Errorf("expected %d active notifications after re-subscribe, got %d", len(symbolNames), resubCount)
	}
	t.Logf("re-subscription successful: %d handles on fresh connection", resubCount)
}

// writeTestCase defines a single write-and-confirm test case.
// altValue is used when the PLC's current value already equals testValue,
// ensuring the test always writes a different value.
type writeTestCase struct {
	envVar    string
	testValue string
	altValue  string
}

var writeTestCases = []writeTestCase{
	// Boolean
	{"ADS_WRITE_BOOL", "true", "false"},
	// Signed integers
	{"ADS_WRITE_SINT", "-42", "100"},
	{"ADS_WRITE_INT", "42", "100"},
	{"ADS_WRITE_DINT", "100000", "200000"},
	// Unsigned integers
	{"ADS_WRITE_USINT", "200", "100"},
	{"ADS_WRITE_UINT", "50000", "30000"},
	{"ADS_WRITE_UDINT", "3000000", "1000000"},
	// Floating point
	{"ADS_WRITE_REAL", "3.14", "6.28"},
	{"ADS_WRITE_LREAL", "2.718281828", "1.414213562"},
	// String
	{"ADS_WRITE_STRING", "hello", "world"},
	// Bit fields
	{"ADS_WRITE_BYTE", "170", "85"},
	{"ADS_WRITE_WORD", "43690", "21845"},
	{"ADS_WRITE_DWORD", "2863311530", "1431655765"},
	// Time types
	{"ADS_WRITE_TIME", "01:23:45.678", "00:00:01"},
	{"ADS_WRITE_DATE", "2024-06-15", "2000-01-01"},
	{"ADS_WRITE_DT", "2024-06-15 13:30:00", "2000-01-01 00:00:00"},
	{"ADS_WRITE_TOD", "13:45", "00:01"},
}

// valuesApproxEqual compares two value strings. For float types (REAL/LREAL),
// it uses approximate comparison to handle float32/float64 round-trip differences.
// The envVar hint (e.g. "ADS_WRITE_REAL") is used when available; otherwise it
// falls back to trying numeric parsing.
func valuesApproxEqual(expected, actual, envVar string) bool {
	if expected == actual {
		return true
	}
	// Use env var hint for known float types
	isFloat := envVar == "ADS_WRITE_REAL" || envVar == "ADS_WRITE_LREAL"
	if !isFloat {
		// Fallback: try parsing both as floats
		_, err1 := strconv.ParseFloat(expected, 64)
		_, err2 := strconv.ParseFloat(actual, 64)
		isFloat = err1 == nil && err2 == nil
	}
	if !isFloat {
		return false
	}
	e, _ := strconv.ParseFloat(expected, 64)
	a, _ := strconv.ParseFloat(actual, 64)
	if e == 0 {
		return math.Abs(a) < 1e-6
	}
	return math.Abs((e-a)/e) < 1e-6
}

func TestIntegrationWriteAndConfirm(t *testing.T) {
	for _, tc := range writeTestCases {
		tc := tc
		symbolName := os.Getenv(tc.envVar)
		if symbolName == "" {
			continue
		}
		t.Run(tc.envVar, func(t *testing.T) {
			conn := setupConnection(t)

			// Load full symbol table so parse() works correctly
			if err := conn.LoadSymbols(); err != nil {
				t.Fatalf("LoadSymbols failed: %v", err)
			}

			// 1. Read current value (save for restore)
			original, err := conn.ReadFromSymbol(symbolName)
			if err != nil {
				t.Fatalf("ReadFromSymbol(%q) failed: %v", symbolName, err)
			}
			t.Logf("original value of %s = %s", symbolName, original)

			// Pick a write value that differs from the original
			writeValue := tc.testValue
			if writeValue == original {
				writeValue = tc.altValue
				t.Logf("original equals testValue, using altValue %q", writeValue)
			}

			// 2. Write test value
			err = conn.WriteToSymbol(symbolName, writeValue)
			if err != nil {
				t.Fatalf("WriteToSymbol(%q, %q) failed: %v", symbolName, writeValue, err)
			}

			// 3. Read back and assert it changed
			readBack, err := conn.ReadFromSymbol(symbolName)
			if err != nil {
				t.Fatalf("ReadFromSymbol(%q) after write failed: %v", symbolName, err)
			}
			t.Logf("wrote %q, read back %q", writeValue, readBack)

			if !valuesApproxEqual(writeValue, readBack, tc.envVar) {
				t.Errorf("write-confirm mismatch: wrote %q but read back %q", writeValue, readBack)
			}

			// 4. Restore original value and confirm
			err = conn.WriteToSymbol(symbolName, original)
			if err != nil {
				t.Fatalf("failed to restore original value %q to %s: %v", original, symbolName, err)
			}
			restored, err := conn.ReadFromSymbol(symbolName)
			if err != nil {
				t.Fatalf("ReadFromSymbol(%q) after restore failed: %v", symbolName, err)
			}
			if !valuesApproxEqual(original, restored, tc.envVar) {
				t.Errorf("restore mismatch: expected %q but read back %q", original, restored)
			}
			t.Logf("restored %s = %s", symbolName, restored)
		})
	}
}

func TestIntegrationWriteMultipleSymbols(t *testing.T) {
	// Collect symbols that have env vars set
	type symbolPair struct {
		name      string
		testValue string
		altValue  string
	}
	var pairs []symbolPair
	for _, tc := range writeTestCases {
		name := os.Getenv(tc.envVar)
		if name != "" {
			pairs = append(pairs, symbolPair{name: name, testValue: tc.testValue, altValue: tc.altValue})
		}
	}
	if len(pairs) < 2 {
		t.Skip("need at least 2 ADS_WRITE_* env vars set for WriteMultipleSymbols test")
	}

	conn := setupConnection(t)

	// Load full symbol table so parse() works correctly
	if err := conn.LoadSymbols(); err != nil {
		t.Fatalf("LoadSymbols failed: %v", err)
	}

	// 1. Read current values (save for restore)
	originals := make(map[string]string)
	for _, p := range pairs {
		val, err := conn.ReadFromSymbol(p.name)
		if err != nil {
			t.Fatalf("ReadFromSymbol(%q) failed: %v", p.name, err)
		}
		originals[p.name] = val
		t.Logf("original %s = %s", p.name, val)
	}

	// 2. Build write values, ensuring each differs from the original
	writeValues := make(map[string]string)
	for _, p := range pairs {
		v := p.testValue
		if v == originals[p.name] {
			v = p.altValue
		}
		writeValues[p.name] = v
	}

	// 3. Write all via WriteMultipleSymbols
	codes, err := conn.WriteMultipleSymbols(writeValues)
	if err != nil {
		t.Fatalf("WriteMultipleSymbols failed: %v", err)
	}

	// 4. Check per-symbol return codes
	for name, code := range codes {
		if code != ReturnCodeNoErrors {
			t.Errorf("WriteMultipleSymbols: %s returned error code %d", name, code)
		}
	}

	// 5. Read back each, assert it changed to the written value
	for _, p := range pairs {
		expected := writeValues[p.name]
		readBack, err := conn.ReadFromSymbol(p.name)
		if err != nil {
			t.Errorf("ReadFromSymbol(%q) after batch write failed: %v", p.name, err)
			continue
		}
		if !valuesApproxEqual(expected, readBack, "") {
			t.Errorf("batch write-confirm mismatch for %s: wrote %q but read back %q", p.name, expected, readBack)
		}
		t.Logf("confirmed %s = %s", p.name, readBack)
	}

	// 6. Restore originals and confirm
	restoreCodes, err := conn.WriteMultipleSymbols(originals)
	if err != nil {
		t.Fatalf("failed to restore originals: %v", err)
	}
	for name, code := range restoreCodes {
		if code != ReturnCodeNoErrors {
			t.Errorf("restore %s returned error code %d", name, code)
		}
	}
	for _, p := range pairs {
		restored, err := conn.ReadFromSymbol(p.name)
		if err != nil {
			t.Errorf("ReadFromSymbol(%q) after restore failed: %v", p.name, err)
			continue
		}
		if !valuesApproxEqual(originals[p.name], restored, "") {
			t.Errorf("restore mismatch for %s: expected %q but read back %q", p.name, originals[p.name], restored)
		}
		t.Logf("restored %s = %s", p.name, restored)
	}
}

func TestIntegrationReadMultipleSymbols(t *testing.T) {
	conn := setupConnection(t)

	err := conn.LoadSymbols()
	if err != nil {
		t.Fatalf("LoadSymbols failed: %v", err)
	}

	symbols, _ := conn.ListSymbols()
	names := pickParseableSymbols(symbols, 5)
	if len(names) < 2 {
		t.Skip("need at least 2 parseable symbols for ReadMultipleSymbols test")
	}

	values, err := conn.ReadMultipleSymbols(names)
	if err != nil {
		t.Fatalf("ReadMultipleSymbols failed: %v", err)
	}

	if len(values) != len(names) {
		t.Errorf("expected %d results from ReadMultipleSymbols, got %d", len(names), len(values))
	}

	for name, val := range values {
		t.Logf("  %s = %s", name, val)
	}

	// Verify each returned value matches an individual read
	for name, batchVal := range values {
		singleVal, err := conn.ReadFromSymbol(name)
		if err != nil {
			t.Errorf("ReadFromSymbol(%q) failed: %v", name, err)
			continue
		}
		if !valuesApproxEqual(batchVal, singleVal, "") {
			t.Errorf("batch/single mismatch for %s: batch=%q single=%q", name, batchVal, singleVal)
		}
	}
}

func TestIntegrationLoadSymbolsSlow(t *testing.T) {
	conn := setupConnection(t)

	err := conn.LoadSymbolsSlow(SlowDiscoveryConfig{
		ChunkSize:  2048,
		ChunkDelay: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("LoadSymbolsSlow failed: %v", err)
	}

	symbols, err := conn.ListSymbols()
	if err != nil {
		t.Fatalf("ListSymbols failed: %v", err)
	}
	if len(symbols) == 0 {
		t.Fatal("expected at least one symbol after LoadSymbolsSlow")
	}
	t.Logf("LoadSymbolsSlow found %d symbols", len(symbols))

	// Verify a symbol can be read
	name := pickParseableSymbol(symbols)
	if name == "" {
		t.Skip("no parseable symbols available")
	}
	value, err := conn.ReadFromSymbol(name)
	if err != nil {
		t.Fatalf("ReadFromSymbol(%q) after slow load failed: %v", name, err)
	}
	t.Logf("read %s = %s", name, value)
}

func TestIntegrationCheckSymbolVersion(t *testing.T) {
	conn := setupConnection(t)

	changed, err := conn.CheckSymbolVersion()
	if err != nil {
		// Some PLCs (e.g. TwinCAT 2) don't support GroupSymbolVersion
		t.Skipf("CheckSymbolVersion not supported on this PLC: %v", err)
	}
	// On first call after connect, the stored version should match what Connect() read
	if changed {
		t.Log("symbol version changed (unexpected but not fatal)")
	} else {
		t.Log("symbol version unchanged (expected)")
	}

	// Call again — should still be unchanged
	changed2, err := conn.CheckSymbolVersion()
	if err != nil {
		t.Fatalf("second CheckSymbolVersion failed: %v", err)
	}
	if changed2 {
		t.Error("symbol version should not change between two consecutive calls")
	}
}

func TestIntegrationReadProcessData(t *testing.T) {
	conn := setupConnection(t)

	if err := conn.LoadSymbols(); err != nil {
		t.Fatalf("LoadSymbols failed: %v", err)
	}

	// Read counter twice with delay to verify live data
	counterName := os.Getenv("ADS_READ_COUNTER")
	if counterName != "" {
		val1, err := conn.ReadFromSymbol(counterName)
		if err != nil {
			t.Fatalf("ReadFromSymbol(%q) failed: %v", counterName, err)
		}
		t.Logf("counter read 1: %s = %s", counterName, val1)

		time.Sleep(1100 * time.Millisecond) // counter updates every cycle (10ms), but value may be cached

		val2, err := conn.ReadFromSymbol(counterName)
		if err != nil {
			t.Fatalf("ReadFromSymbol(%q) second read failed: %v", counterName, err)
		}
		t.Logf("counter read 2: %s = %s", counterName, val2)

		// Parse as integers and verify increment
		n1, err1 := strconv.ParseUint(val1, 10, 64)
		n2, err2 := strconv.ParseUint(val2, 10, 64)
		if err1 == nil && err2 == nil {
			if n2 <= n1 {
				t.Errorf("counter did not increment: %d -> %d", n1, n2)
			} else {
				t.Logf("counter incremented: %d -> %d (delta=%d)", n1, n2, n2-n1)
			}
		}
	}

	// Read a REAL value
	realName := os.Getenv("ADS_READ_REAL")
	if realName != "" {
		val, err := conn.ReadFromSymbol(realName)
		if err != nil {
			t.Fatalf("ReadFromSymbol(%q) failed: %v", realName, err)
		}
		t.Logf("real: %s = %s", realName, val)
		// Verify it parses as a float
		if _, err := strconv.ParseFloat(val, 64); err != nil {
			t.Errorf("expected float value for %s, got %q", realName, val)
		}
	}

	// Read a STRING value
	stringName := os.Getenv("ADS_READ_STRING")
	if stringName != "" {
		val, err := conn.ReadFromSymbol(stringName)
		if err != nil {
			t.Fatalf("ReadFromSymbol(%q) failed: %v", stringName, err)
		}
		t.Logf("string: %s = %q", stringName, val)
		if val == "" {
			t.Errorf("expected non-empty string for %s", stringName)
		}
	}

	if counterName == "" && realName == "" && stringName == "" {
		t.Skip("no ADS_READ_* env vars set")
	}
}

// waitForReconnect waits for the full reconnect cycle: first waits for the
// connection to become disconnected (confirming the error was detected), then
// waits for reconnect to fully complete (disconnected=false AND reconnecting=false).
func waitForReconnect(t *testing.T, conn *Connection, timeout time.Duration) {
	t.Helper()
	deadline := time.After(timeout)
	tick := time.NewTicker(50 * time.Millisecond)
	defer tick.Stop()

	// Phase 1: wait for disconnect to be detected
	for !conn.IsDisconnected() && !conn.lifecycle.reconnecting.Load() {
		select {
		case <-deadline:
			t.Fatal("reconnect was never triggered within timeout")
		case <-tick.C:
		}
	}

	// Phase 2: wait for reconnect to fully complete
	for conn.IsDisconnected() || conn.lifecycle.reconnecting.Load() {
		select {
		case <-deadline:
			t.Fatalf("reconnect did not complete within timeout (disconnected=%v, reconnecting=%v)",
				conn.IsDisconnected(), conn.lifecycle.reconnecting.Load())
		case <-tick.C:
		}
	}
}

func TestIntegrationReconnect(t *testing.T) {
	conn := setupConnection(t)

	if err := conn.LoadSymbols(); err != nil {
		t.Fatalf("LoadSymbols failed: %v", err)
	}

	symbols, _ := conn.ListSymbols()
	symbolName := pickParseableSymbol(symbols)
	if symbolName == "" {
		t.Skip("no parseable symbols available")
	}

	// 1. Read symbol to confirm connection works
	val1, err := conn.ReadFromSymbol(symbolName)
	if err != nil {
		t.Fatalf("pre-reconnect ReadFromSymbol(%q) failed: %v", symbolName, err)
	}
	t.Logf("pre-reconnect: %s = %s", symbolName, val1)

	// 2. Subscribe to notification
	ch := make(chan *Update, 10)
	handle, err := conn.AddSymbolNotification(symbolName, 100*time.Millisecond, 100*time.Millisecond, TransModeServerOnChange, ch)
	if err != nil {
		t.Fatalf("AddSymbolNotification failed: %v", err)
	}
	t.Logf("notification handle: %d", handle)

	// Wait for initial notification to confirm subscription works
	select {
	case update := <-ch:
		t.Logf("pre-reconnect notification: %s = %s", update.Variable, update.Value)
	case <-time.After(3 * time.Second):
		t.Log("no pre-reconnect notification (continuing)")
	}

	// 3. Simulate network drop by closing TCP connection.
	// Expect "listen read error, triggering reconnect" in logs — this is the
	// detection mechanism firing after we deliberately close the socket.
	t.Log("simulating network drop (expect 'listen read error' log)...")
	conn.tx.connMu.Lock()
	conn.tx.connection.Close()
	conn.tx.connMu.Unlock()

	// 4. Wait for reconnect to complete
	waitForReconnect(t, conn, 15*time.Second)
	t.Log("reconnect completed")

	// 5. Read symbol again — must succeed after reconnect
	val2, err := conn.ReadFromSymbol(symbolName)
	if err != nil {
		t.Fatalf("post-reconnect ReadFromSymbol(%q) failed: %v", symbolName, err)
	}
	t.Logf("post-reconnect: %s = %s", symbolName, val2)

	// 6. Wait for notification — proves notifications were re-subscribed
	select {
	case update := <-ch:
		t.Logf("post-reconnect notification: %s = %s", update.Variable, update.Value)
	case <-time.After(5 * time.Second):
		t.Log("no post-reconnect notification within 5s (may be expected if value doesn't change)")
	}
}

func TestIntegrationReconnectDuringBatchRead(t *testing.T) {
	conn := setupConnection(t)

	if err := conn.LoadSymbols(); err != nil {
		t.Fatalf("LoadSymbols failed: %v", err)
	}

	symbols, _ := conn.ListSymbols()
	names := pickParseableSymbols(symbols, 5)
	if len(names) < 2 {
		t.Skip("need at least 2 parseable symbols")
	}

	// 1. Successful batch read before reconnect
	values1, err := conn.ReadMultipleSymbols(names)
	if err != nil {
		t.Fatalf("pre-reconnect ReadMultipleSymbols failed: %v", err)
	}
	t.Logf("pre-reconnect batch read: %d symbols", len(values1))
	for name, val := range values1 {
		t.Logf("  %s = %s", name, val)
	}

	// 2. Simulate network drop.
	// Expect "listen read error, triggering reconnect" in logs — this is the
	// detection mechanism firing after we deliberately close the socket.
	t.Log("simulating network drop (expect 'listen read error' log)...")
	conn.tx.connMu.Lock()
	conn.tx.connection.Close()
	conn.tx.connMu.Unlock()

	// 3. Wait for reconnect
	waitForReconnect(t, conn, 15*time.Second)
	t.Log("reconnect completed")

	// 4. Batch read again — must succeed, proving handles and SumRead work after reconnect
	values2, err := conn.ReadMultipleSymbols(names)
	if err != nil {
		t.Fatalf("post-reconnect ReadMultipleSymbols failed: %v", err)
	}
	t.Logf("post-reconnect batch read: %d symbols", len(values2))
	for name, val := range values2 {
		t.Logf("  %s = %s", name, val)
	}

	if len(values2) != len(names) {
		t.Errorf("expected %d results after reconnect, got %d", len(names), len(values2))
	}
}

// TestIntegrationReconnectReadDuringDisconnect verifies that sendRequest's
// built-in retry handles the race window between TCP death and reconnect
// completion. Instead of waiting for reconnect first, we issue a read
// immediately after killing the connection — the library must retry
// transparently.
func TestIntegrationReconnectReadDuringDisconnect(t *testing.T) {
	conn := setupConnection(t)

	if err := conn.LoadSymbols(); err != nil {
		t.Fatalf("LoadSymbols failed: %v", err)
	}

	symbols, _ := conn.ListSymbols()
	symbolName := pickParseableSymbol(symbols)
	if symbolName == "" {
		t.Skip("no parseable symbols available")
	}

	// 1. Confirm connection works
	val1, err := conn.ReadFromSymbol(symbolName)
	if err != nil {
		t.Fatalf("pre-disconnect ReadFromSymbol(%q) failed: %v", symbolName, err)
	}
	t.Logf("pre-disconnect: %s = %s", symbolName, val1)

	// 2. Kill TCP — triggers reconnect in background.
	// Expect "listen read error, triggering reconnect" in logs — this is the
	// detection mechanism firing after we deliberately close the socket.
	t.Log("simulating network drop (expect 'listen read error' log)...")
	conn.tx.connMu.Lock()
	conn.tx.connection.Close()
	conn.tx.connMu.Unlock()

	// 3. Immediately read WITHOUT waiting for reconnect.
	// sendRequest's retry loop should handle this transparently.
	val2, err := conn.ReadFromSymbol(symbolName)
	if err != nil {
		t.Fatalf("read during reconnect failed (sendRequest retry should have handled this): %v", err)
	}
	t.Logf("read during reconnect succeeded: %s = %s", symbolName, val2)

	// 4. Wait for reconnect to fully complete before Close() runs,
	// so handle cleanup can succeed cleanly.
	waitForReconnect(t, conn, 15*time.Second)
}

func TestIntegrationBatchNotification(t *testing.T) {
	conn := setupConnection(t)

	if err := conn.LoadSymbols(); err != nil {
		t.Fatalf("LoadSymbols failed: %v", err)
	}
	symbols, _ := conn.ListSymbols()
	names := pickParseableSymbols(symbols, 3)
	if len(names) < 2 {
		t.Skip("need at least 2 parseable symbols")
	}

	ch := make(chan *Update, 50)

	var configs []NotificationConfig
	for _, name := range names {
		configs = append(configs, NotificationConfig{
			SymbolName:       name,
			MaxDelay:         100 * time.Millisecond,
			CycleTime:        100 * time.Millisecond,
			TransmissionMode: TransModeServerOnChange,
		})
	}

	_, err := conn.AddSymbolNotifications(configs, ch)
	if err != nil {
		t.Fatalf("AddSymbolNotifications failed: %v", err)
	}

	// Verify all handles tracked
	conn.notifs.lock.Lock()
	activeCount := len(conn.notifs.activeNotifications)
	conn.notifs.lock.Unlock()
	if activeCount != len(names) {
		t.Errorf("expected %d active notifications, got %d", len(names), activeCount)
	}
	t.Logf("batch added %d notifications successfully", activeCount)

	// Wait for at least one notification
	select {
	case update := <-ch:
		t.Logf("received notification: %s = %s", update.Variable, update.Value)
	case <-time.After(5 * time.Second):
		t.Log("no notification received within 5s (value may not be changing)")
	}

	// Batch delete via SumDeleteDeviceNotification
	conn.notifs.lock.Lock()
	var handles []uint32
	for h := range conn.notifs.activeNotifications {
		handles = append(handles, h)
	}
	conn.notifs.lock.Unlock()

	codes, err := conn.SumDeleteDeviceNotification(handles)
	if err != nil {
		t.Fatalf("SumDeleteDeviceNotification failed: %v", err)
	}
	for i, code := range codes {
		if code != ReturnCodeNoErrors {
			t.Errorf("delete handle %d returned error: 0x%X", handles[i], uint32(code))
		}
	}

	conn.notifs.lock.Lock()
	finalCount := len(conn.notifs.activeNotifications)
	conn.notifs.lock.Unlock()
	if finalCount != 0 {
		t.Errorf("expected 0 active notifications after batch delete, got %d", finalCount)
	}
	t.Logf("batch deleted %d notifications successfully", len(handles))
}

func TestIntegrationProbeSumCommands(t *testing.T) {
	conn := setupConnection(t)

	if err := conn.LoadSymbols(); err != nil {
		t.Fatalf("LoadSymbols failed: %v", err)
	}

	symbols, _ := conn.ListSymbols()
	names := pickParseableSymbols(symbols, 3)
	if len(names) < 2 {
		t.Skip("need at least 2 parseable symbols")
	}

	// Build requests for 2 symbols
	type symReq struct {
		name   string
		symbol *Symbol
		group  uint32
		offset uint32
	}
	var reqs []symReq
	for _, name := range names[:2] {
		sym, err := conn.getSymbol(name)
		if err != nil {
			t.Fatalf("getSymbol(%s): %v", name, err)
		}
		g, o := symbolSumAddress(sym)
		reqs = append(reqs, symReq{name: name, symbol: sym, group: g, offset: o})
		t.Logf("symbol: %s (type=%s, length=%d, group=0x%X, offset=0x%X)",
			name, sym.DataType, sym.Length, g, o)
	}

	n := len(reqs)

	// Build write data for sum read commands: N × 12 bytes
	writeData := make([]byte, n*12)
	var totalLen uint32
	for i, r := range reqs {
		binary.LittleEndian.PutUint32(writeData[i*12:], r.group)
		binary.LittleEndian.PutUint32(writeData[i*12+4:], r.offset)
		binary.LittleEndian.PutUint32(writeData[i*12+8:], r.symbol.Length)
		totalLen += r.symbol.Length
	}

	// Probe each sum read command
	sumCommands := []struct {
		name  string
		group Group
		// readLen for SumRead (0xF080): N*4 (errors) + data
		// readLen for SumReadEx2 (0xF084): N*8 (error+length pairs) + data
	}{
		{"SumRead (0xF080)", GroupSumupRead},
		{"SumReadEx (0xF083)", GroupSumupReadEx},
		{"SumReadEx2 (0xF084)", GroupSumupReadEx2},
	}

	for _, cmd := range sumCommands {
		var readLen uint32
		switch cmd.group {
		case GroupSumupRead:
			readLen = uint32(n*4) + totalLen // [n errors][data]
		case GroupSumupReadEx, GroupSumupReadEx2:
			readLen = uint32(n*8) + totalLen // [n*(error,length)][data]
		}

		resp, err := conn.WriteRead(uint32(cmd.group), uint32(n), readLen, writeData)
		if err != nil {
			t.Logf("%s: NOT SUPPORTED (error: %v)", cmd.name, err)
			continue
		}

		t.Logf("%s: SUPPORTED (response %d bytes)", cmd.name, len(resp))
		// Hex dump first 64 bytes for format analysis
		dumpLen := len(resp)
		if dumpLen > 64 {
			dumpLen = 64
		}
		t.Logf("  raw response (first %d bytes): %x", dumpLen, resp[:dumpLen])

		// Try parsing as separate arrays (TC2 format): [n*error(4)][data]
		if cmd.group == GroupSumupRead {
			t.Log("  --- parsing as SumRead format [errors][data] ---")
			for i := 0; i < n; i++ {
				if (i+1)*4 <= len(resp) {
					errCode := binary.LittleEndian.Uint32(resp[i*4:])
					t.Logf("  item[%d] error=0x%X", i, errCode)
				}
			}
			dataStart := n * 4
			for i, r := range reqs {
				end := dataStart + int(r.symbol.Length)
				if end <= len(resp) {
					t.Logf("  item[%d] data=%x", i, resp[dataStart:end])
				}
				dataStart = end
			}
		}

		// Try parsing as interleaved (TC3 format): [n*(error,length)][data]
		if cmd.group == GroupSumupReadEx2 {
			t.Log("  --- parsing as interleaved [error,length] pairs ---")
			for i := 0; i < n; i++ {
				if (i+1)*8 <= len(resp) {
					errCode := binary.LittleEndian.Uint32(resp[i*8:])
					length := binary.LittleEndian.Uint32(resp[i*8+4:])
					t.Logf("  item[%d] error=0x%X, length=%d", i, errCode, length)
				}
			}
		}
	}

	// Probe sum notification commands
	t.Log("--- Probing Sum Notification Commands ---")

	// Build a single notification request
	notifWriteData := make([]byte, 40)
	binary.LittleEndian.PutUint32(notifWriteData[0:], reqs[0].group)
	binary.LittleEndian.PutUint32(notifWriteData[4:], reqs[0].offset)
	binary.LittleEndian.PutUint32(notifWriteData[8:], reqs[0].symbol.Length)
	binary.LittleEndian.PutUint32(notifWriteData[12:], uint32(TransModeServerOnChange))
	binary.LittleEndian.PutUint32(notifWriteData[16:], 1000000) // maxDelay 100ms in 100ns units
	binary.LittleEndian.PutUint32(notifWriteData[20:], 1000000) // cycleTime 100ms in 100ns units

	resp, err := conn.WriteRead(uint32(GroupSumupAddDeviceNotification), 1, 8, notifWriteData)
	if err != nil {
		t.Logf("SumAddDeviceNotification (0xF085): NOT SUPPORTED (error: %v)", err)
	} else {
		t.Logf("SumAddDeviceNotification (0xF085): SUPPORTED (response %d bytes): %x", len(resp), resp)
		if len(resp) >= 8 {
			errCode := binary.LittleEndian.Uint32(resp[0:])
			handle := binary.LittleEndian.Uint32(resp[4:])
			t.Logf("  error=0x%X, handle=%d", errCode, handle)
			// Clean up: delete the notification
			if errCode == 0 && handle != 0 {
				_ = conn.DeleteDeviceNotification(handle)
			}
		}
	}
}

// ============================================================
// Fallback path tests — force atomic flags to bypass sum commands
// ============================================================

func TestIntegrationSumReadFallbackForced(t *testing.T) {
	conn := setupConnection(t)
	if err := conn.LoadSymbols(); err != nil {
		t.Fatalf("LoadSymbols failed: %v", err)
	}
	symbols, _ := conn.ListSymbols()
	names := pickParseableSymbols(symbols, 3)
	if len(names) < 2 {
		t.Skip("need at least 2 parseable symbols")
	}

	// Force fallback: mark sum read as unsupported
	conn.capabilities.SumReadCmdStore(1)

	values, err := conn.ReadMultipleSymbols(names)
	if err != nil {
		t.Fatalf("ReadMultipleSymbols (fallback) failed: %v", err)
	}
	if len(values) != len(names) {
		t.Fatalf("expected %d results, got %d", len(names), len(values))
	}

	// Cross-check each via individual read
	for _, name := range names {
		batchVal, ok := values[name]
		if !ok {
			t.Errorf("missing result for %s", name)
			continue
		}
		singleVal, err := conn.ReadFromSymbol(name)
		if err != nil {
			t.Errorf("ReadFromSymbol(%s) failed: %v", name, err)
			continue
		}
		if !valuesApproxEqual(batchVal, singleVal, "") {
			t.Errorf("%s: fallback=%q vs single=%q", name, batchVal, singleVal)
		}
		t.Logf("%s = %s (fallback matches single)", name, batchVal)
	}
}

func TestIntegrationSumWriteFallbackForced(t *testing.T) {
	type symbolPair struct {
		name      string
		testValue string
		altValue  string
	}
	var pairs []symbolPair
	for _, tc := range writeTestCases {
		name := os.Getenv(tc.envVar)
		if name == "" {
			continue
		}
		pairs = append(pairs, symbolPair{name: name, testValue: tc.testValue, altValue: tc.altValue})
	}
	if len(pairs) < 2 {
		t.Skip("need at least 2 ADS_WRITE_* env vars")
	}

	conn := setupConnection(t)
	if err := conn.LoadSymbols(); err != nil {
		t.Fatalf("LoadSymbols failed: %v", err)
	}

	// Force fallback
	conn.capabilities.SumWriteStateStore(2) // 2 = checked + unsupported (forces fallback)

	// Save originals
	originals := make(map[string]string)
	writeValues := make(map[string]string)
	for _, p := range pairs {
		orig, err := conn.ReadFromSymbol(p.name)
		if err != nil {
			t.Fatalf("ReadFromSymbol(%s) failed: %v", p.name, err)
		}
		originals[p.name] = orig
		v := p.testValue
		if v == orig {
			v = p.altValue
		}
		writeValues[p.name] = v
	}

	// Batch write in fallback mode
	codes, err := conn.WriteMultipleSymbols(writeValues)
	if err != nil {
		t.Fatalf("WriteMultipleSymbols (fallback) failed: %v", err)
	}
	for name, code := range codes {
		if code != ReturnCodeNoErrors {
			t.Errorf("write %s returned error: 0x%X", name, uint32(code))
		}
	}

	// Verify each individually
	for name, expected := range writeValues {
		actual, err := conn.ReadFromSymbol(name)
		if err != nil {
			t.Errorf("ReadFromSymbol(%s) failed: %v", name, err)
			continue
		}
		if !valuesApproxEqual(expected, actual, "") {
			t.Errorf("%s: wrote %q, read %q", name, expected, actual)
		}
	}

	// Restore
	_, _ = conn.WriteMultipleSymbols(originals)
}

func TestIntegrationSumNotifFallbackForced(t *testing.T) {
	conn := setupConnection(t)
	if err := conn.LoadSymbols(); err != nil {
		t.Fatalf("LoadSymbols failed: %v", err)
	}
	symbols, _ := conn.ListSymbols()
	names := pickParseableSymbols(symbols, 2)
	if len(names) < 2 {
		t.Skip("need at least 2 parseable symbols")
	}

	// Force fallback
	conn.capabilities.SumAddNotifStateStore(2) // 2 = checked + unsupported (forces fallback)

	ch := make(chan *Update, 50)
	var configs []NotificationConfig
	for _, name := range names {
		configs = append(configs, NotificationConfig{
			SymbolName:       name,
			MaxDelay:         100 * time.Millisecond,
			CycleTime:        100 * time.Millisecond,
			TransmissionMode: TransModeServerOnChange,
		})
	}

	_, err := conn.AddSymbolNotifications(configs, ch)
	if err != nil {
		t.Fatalf("AddSymbolNotifications (fallback) failed: %v", err)
	}

	// Verify handles tracked
	conn.notifs.lock.Lock()
	activeCount := len(conn.notifs.activeNotifications)
	var handles []uint32
	for h := range conn.notifs.activeNotifications {
		handles = append(handles, h)
	}
	conn.notifs.lock.Unlock()

	if activeCount != len(names) {
		t.Errorf("expected %d active notifications, got %d", len(names), activeCount)
	}
	t.Logf("fallback added %d notifications", activeCount)

	// Wait for a notification
	select {
	case u := <-ch:
		t.Logf("received: %s = %s", u.Variable, u.Value)
	case <-time.After(3 * time.Second):
		t.Log("no notification within 3s (continuing)")
	}

	// Batch delete in fallback mode
	codes, err := conn.SumDeleteDeviceNotification(handles)
	if err != nil {
		t.Fatalf("SumDeleteDeviceNotification (fallback) failed: %v", err)
	}
	for i, code := range codes {
		if code != ReturnCodeNoErrors {
			t.Errorf("delete handle %d error: 0x%X", handles[i], uint32(code))
		}
	}
}

func TestIntegrationSumNotifFallbackDowngrade(t *testing.T) {
	conn := setupConnection(t)
	if err := conn.LoadSymbols(); err != nil {
		t.Fatalf("LoadSymbols failed: %v", err)
	}
	symbols, _ := conn.ListSymbols()
	symbolName := pickParseableSymbol(symbols)
	if symbolName == "" {
		t.Skip("no parseable symbol")
	}

	// Check ContextMask — with ContextMask=0, InContext mode gets downgraded at AddSymbolNotifications level.
	// Then we also force the sum command fallback path which has its own downgrade.
	sym, err := conn.GetSymbol(symbolName)
	if err != nil {
		t.Fatalf("GetSymbol failed: %v", err)
	}
	t.Logf("symbol %q: ContextMask=%d flags=0x%04X (fallback test)", symbolName, sym.ContextMask, uint32(sym.Flags))

	// Force notification fallback — v2 modes should be downgraded to v1
	conn.capabilities.SumAddNotifStateStore(2) // 2 = checked + unsupported (forces fallback)

	ch := make(chan *Update, 20)
	configs := []NotificationConfig{{
		SymbolName:       symbolName,
		MaxDelay:         100 * time.Millisecond,
		CycleTime:        100 * time.Millisecond,
		TransmissionMode: TransModeServerCycle2, // should be downgraded to ServerCycle
	}}

	_, err = conn.AddSymbolNotifications(configs, ch)
	if err != nil {
		t.Fatalf("AddSymbolNotifications (downgrade) failed: %v", err)
	}

	// Collect — ServerCycle (downgraded) should deliver periodic updates
	var count int
	timeout := time.After(3 * time.Second)
	for {
		select {
		case u := <-ch:
			count++
			if count == 1 {
				t.Logf("first notification after downgrade: %s = %s", u.Variable, u.Value)
			}
		case <-timeout:
			goto done
		}
	}
done:
	t.Logf("received %d notifications via downgraded ServerCycle2→ServerCycle", count)
	if count < 2 {
		t.Errorf("expected periodic notifications from downgraded cycle mode, got %d", count)
	}

	// Cleanup: copy handles first to avoid mutating map during iteration
	conn.notifs.lock.Lock()
	handles := make([]uint32, 0, len(conn.notifs.activeNotifications))
	for h := range conn.notifs.activeNotifications {
		handles = append(handles, h)
	}
	conn.notifs.lock.Unlock()
	for _, h := range handles {
		_ = conn.DeleteDeviceNotification(h)
	}
}

// ============================================================
// Partial batch failure tests
// ============================================================

func TestIntegrationSumReadPartialFailure(t *testing.T) {
	conn := setupConnection(t)
	if err := conn.LoadSymbols(); err != nil {
		t.Fatalf("LoadSymbols failed: %v", err)
	}
	symbols, _ := conn.ListSymbols()
	name := pickParseableSymbol(symbols)
	if name == "" {
		t.Skip("no parseable symbol")
	}

	sym, err := conn.getSymbol(name)
	if err != nil {
		t.Fatalf("getSymbol(%s) failed: %v", name, err)
	}
	validGroup, validOffset := symbolSumAddress(sym)

	requests := []SumReadRequest{
		{Group: validGroup, Offset: validOffset, Length: sym.Length}, // valid
		{Group: 0xFFFF, Offset: 0xFFFFFFFF, Length: 4},               // bogus
	}

	results, err := conn.SumRead(requests)
	if err != nil {
		t.Fatalf("SumRead failed: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}

	if results[0].Error != ReturnCodeNoErrors {
		t.Errorf("valid request returned error: 0x%X", uint32(results[0].Error))
	}
	if len(results[0].Data) != int(sym.Length) {
		t.Errorf("valid request data length: got %d, want %d", len(results[0].Data), sym.Length)
	}
	t.Logf("valid read: %d bytes, error=0x%X", len(results[0].Data), uint32(results[0].Error))

	if results[1].Error == ReturnCodeNoErrors {
		t.Error("bogus request should have returned an error")
	}
	t.Logf("bogus read error: 0x%X", uint32(results[1].Error))
}

func TestIntegrationSumWritePartialFailure(t *testing.T) {
	symbolName := os.Getenv("ADS_WRITE_INT")
	if symbolName == "" {
		t.Skip("ADS_WRITE_INT not set")
	}

	conn := setupConnection(t)
	if err := conn.LoadSymbols(); err != nil {
		t.Fatalf("LoadSymbols failed: %v", err)
	}

	sym, err := conn.getSymbol(symbolName)
	if err != nil {
		t.Fatalf("getSymbol(%s) failed: %v", symbolName, err)
	}

	original, err := conn.ReadFromSymbol(symbolName)
	if err != nil {
		t.Fatalf("ReadFromSymbol failed: %v", err)
	}

	// Step 1: Verify SumWrite works via WriteMultipleSymbols (proven path).
	writeVal := "42"
	if original == "42" {
		writeVal = "100"
	}
	codes, err := conn.WriteMultipleSymbols(map[string]string{symbolName: writeVal})
	if err != nil {
		t.Fatalf("WriteMultipleSymbols failed: %v", err)
	}
	if codes[symbolName] != ReturnCodeNoErrors {
		t.Fatalf("WriteMultipleSymbols returned error: 0x%X", uint32(codes[symbolName]))
	}
	readBack, _ := conn.ReadFromSymbol(symbolName)
	if !valuesApproxEqual(writeVal, readBack, "ADS_WRITE_INT") {
		t.Fatalf("SumWrite (via WriteMultipleSymbols) not working: wrote %q, read %q", writeVal, readBack)
	}
	t.Logf("SumWrite confirmed working: %s = %s", symbolName, readBack)

	// Restore before mixed test
	_ = conn.WriteToSymbol(symbolName, original)

	// Step 2: Now test mixed valid + bogus in one batch.
	validGroup, validOffset := symbolSumAddress(sym)
	mixedWriteVal := writeVal

	// Use connection datatypes (same as WriteMultipleSymbols does)
	conn.cache.lock.Lock()
	datatypes := conn.cache.datatypes
	conn.cache.lock.Unlock()
	mixedData, _ := sym.writeToNode(mixedWriteVal, datatypes)

	requests := []SumWriteRequest{
		{Group: validGroup, Offset: validOffset, Data: mixedData}, // valid
		{Group: 0xFFFF, Offset: 0xFFFFFFFF, Data: []byte{0, 0}},   // bogus
	}

	results, err := conn.SumWrite(requests)
	if err != nil {
		t.Fatalf("SumWrite (mixed) failed: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}

	// Bogus entry must have an error
	if results[1].Error == ReturnCodeNoErrors {
		t.Error("bogus write should have returned an error")
	}
	t.Logf("mixed batch: valid=0x%X, bogus=0x%X", uint32(results[0].Error), uint32(results[1].Error))

	// Check if valid write in mixed batch was actually applied.
	// Some PLCs roll back the entire batch when any entry fails (atomic behavior).
	// Others apply entries independently. Both are valid PLC implementations.
	mixedReadBack, _ := conn.ReadFromSymbol(symbolName)
	if valuesApproxEqual(mixedWriteVal, mixedReadBack, "ADS_WRITE_INT") {
		t.Logf("mixed batch: valid write applied independently (non-atomic)")
	} else {
		t.Logf("mixed batch: valid write rolled back (atomic batch behavior) — wrote %q, read %q", mixedWriteVal, mixedReadBack)
	}

	// Restore
	_ = conn.WriteToSymbol(symbolName, original)
}

// ============================================================
// SumWrite raw debug diagnostic
// ============================================================


// ============================================================
// SumRead/SumWrite functional verification
// ============================================================

func TestIntegrationSumWriteVerifyData(t *testing.T) {
	type symbolPair struct {
		name      string
		testValue string
		altValue  string
		envVar    string
	}
	var pairs []symbolPair
	for _, tc := range writeTestCases {
		name := os.Getenv(tc.envVar)
		if name == "" {
			continue
		}
		pairs = append(pairs, symbolPair{name: name, testValue: tc.testValue, altValue: tc.altValue, envVar: tc.envVar})
	}
	if len(pairs) < 2 {
		t.Skip("need at least 2 ADS_WRITE_* env vars")
	}

	conn := setupConnection(t)
	if err := conn.LoadSymbols(); err != nil {
		t.Fatalf("LoadSymbols failed: %v", err)
	}

	// Save originals
	originals := make(map[string]string)
	writeValues := make(map[string]string)
	envVarMap := make(map[string]string)
	for _, p := range pairs {
		orig, err := conn.ReadFromSymbol(p.name)
		if err != nil {
			t.Fatalf("ReadFromSymbol(%s) failed: %v", p.name, err)
		}
		originals[p.name] = orig
		v := p.testValue
		if v == orig {
			v = p.altValue
		}
		writeValues[p.name] = v
		envVarMap[p.name] = p.envVar
	}

	// Batch write
	codes, err := conn.WriteMultipleSymbols(writeValues)
	if err != nil {
		t.Fatalf("WriteMultipleSymbols failed: %v", err)
	}
	for name, code := range codes {
		if code != ReturnCodeNoErrors {
			t.Errorf("write %s error: 0x%X", name, uint32(code))
		}
	}

	// Verify each INDIVIDUALLY (not batch) to confirm SumWrite correctness
	for name, expected := range writeValues {
		actual, err := conn.ReadFromSymbol(name)
		if err != nil {
			t.Errorf("ReadFromSymbol(%s) failed: %v", name, err)
			continue
		}
		if !valuesApproxEqual(expected, actual, envVarMap[name]) {
			t.Errorf("%s: wrote %q via SumWrite, read %q individually", name, expected, actual)
		} else {
			t.Logf("%s: SumWrite %q confirmed", name, expected)
		}
	}

	// Restore
	_, _ = conn.WriteMultipleSymbols(originals)
}

func TestIntegrationSumReadKnownValues(t *testing.T) {
	type symbolPair struct {
		name      string
		testValue string
		altValue  string
		envVar    string
	}
	var pairs []symbolPair
	for _, tc := range writeTestCases {
		name := os.Getenv(tc.envVar)
		if name == "" {
			continue
		}
		pairs = append(pairs, symbolPair{name: name, testValue: tc.testValue, altValue: tc.altValue, envVar: tc.envVar})
	}
	if len(pairs) < 2 {
		t.Skip("need at least 2 ADS_WRITE_* env vars")
	}

	conn := setupConnection(t)
	if err := conn.LoadSymbols(); err != nil {
		t.Fatalf("LoadSymbols failed: %v", err)
	}

	// Save originals and write known values individually
	originals := make(map[string]string)
	knownValues := make(map[string]string)
	envVarMap := make(map[string]string)
	for _, p := range pairs {
		orig, err := conn.ReadFromSymbol(p.name)
		if err != nil {
			t.Fatalf("ReadFromSymbol(%s) failed: %v", p.name, err)
		}
		originals[p.name] = orig
		v := p.testValue
		if v == orig {
			v = p.altValue
		}
		err = conn.WriteToSymbol(p.name, v)
		if err != nil {
			t.Fatalf("WriteToSymbol(%s, %s) failed: %v", p.name, v, err)
		}
		knownValues[p.name] = v
		envVarMap[p.name] = p.envVar
	}

	// Batch read all
	names := make([]string, 0, len(knownValues))
	for name := range knownValues {
		names = append(names, name)
	}
	batchValues, err := conn.ReadMultipleSymbols(names)
	if err != nil {
		t.Fatalf("ReadMultipleSymbols failed: %v", err)
	}

	// Verify each matches the known written value
	for name, expected := range knownValues {
		actual, ok := batchValues[name]
		if !ok {
			t.Errorf("missing batch result for %s", name)
			continue
		}
		if !valuesApproxEqual(expected, actual, envVarMap[name]) {
			t.Errorf("%s: wrote %q individually, SumRead returned %q", name, expected, actual)
		} else {
			t.Logf("%s: SumRead %q matches written value", name, actual)
		}
	}

	// Restore
	for name, orig := range originals {
		_ = conn.WriteToSymbol(name, orig)
	}
}

// ============================================================
// 64-bit integration tests
// ============================================================

func TestIntegrationWrite64BitTypes(t *testing.T) {
	testCases := []struct {
		envVar    string
		testValue string
		altValue  string
	}{
		{"ADS_WRITE_LINT", "-9223372036854775000", "42"},
		{"ADS_WRITE_LWORD", "18446744073709551000", "100"},
	}

	var hasAny bool
	for _, tc := range testCases {
		if os.Getenv(tc.envVar) != "" {
			hasAny = true
			break
		}
	}
	if !hasAny {
		t.Skip("no 64-bit type env vars set (ADS_WRITE_LINT/ADS_WRITE_LWORD) — TC2 lacks 64-bit support")
	}

	for _, tc := range testCases {
		tc := tc
		symbolName := os.Getenv(tc.envVar)
		if symbolName == "" {
			t.Logf("skipping %s (env var not set)", tc.envVar)
			continue
		}
		t.Run(tc.envVar, func(t *testing.T) {
			conn := setupConnection(t)
			if err := conn.LoadSymbols(); err != nil {
				t.Fatalf("LoadSymbols failed: %v", err)
			}

			original, err := conn.ReadFromSymbol(symbolName)
			if err != nil {
				t.Skipf("symbol %s not available: %v", symbolName, err)
			}
			t.Logf("original %s = %s", symbolName, original)

			writeValue := tc.testValue
			if writeValue == original {
				writeValue = tc.altValue
			}

			err = conn.WriteToSymbol(symbolName, writeValue)
			if err != nil {
				t.Fatalf("WriteToSymbol(%s, %s) failed: %v", symbolName, writeValue, err)
			}

			readBack, err := conn.ReadFromSymbol(symbolName)
			if err != nil {
				t.Fatalf("ReadFromSymbol after write failed: %v", err)
			}
			if !valuesApproxEqual(writeValue, readBack, tc.envVar) {
				t.Errorf("wrote %q, read %q", writeValue, readBack)
			}
			t.Logf("wrote %s, read back %s", writeValue, readBack)

			_ = conn.WriteToSymbol(symbolName, original)
		})
	}
}

func TestIntegrationRead64BitCounter(t *testing.T) {
	conn := setupConnection(t)
	if err := conn.LoadSymbols(); err != nil {
		t.Fatalf("LoadSymbols failed: %v", err)
	}

	counterName := getEnvOrDefault("ADS_READ_COUNTER", "GVL_ProcessData.nMasterCycleCounter")

	val1, err := conn.ReadFromSymbol(counterName)
	if err != nil {
		t.Skipf("counter %s not available: %v", counterName, err)
	}

	time.Sleep(1100 * time.Millisecond)

	val2, err := conn.ReadFromSymbol(counterName)
	if err != nil {
		t.Fatalf("second read failed: %v", err)
	}

	n1, err1 := strconv.ParseUint(val1, 10, 64)
	n2, err2 := strconv.ParseUint(val2, 10, 64)
	if err1 != nil || err2 != nil {
		t.Logf("counter values: %q -> %q (not uint64, may be DINT on TC2)", val1, val2)
		// Try signed parse for TC2 DINT counters
		s1, _ := strconv.ParseInt(val1, 10, 64)
		s2, _ := strconv.ParseInt(val2, 10, 64)
		if s2 <= s1 {
			t.Errorf("counter did not increment: %d -> %d", s1, s2)
		} else {
			t.Logf("counter (signed): %d -> %d (delta=%d)", s1, s2, s2-s1)
		}
		return
	}

	if n2 <= n1 {
		t.Errorf("ULINT counter did not increment: %d -> %d", n1, n2)
	}
	t.Logf("ULINT counter: %d -> %d (delta=%d)", n1, n2, n2-n1)
}

// ============================================================
// Enum & struct tests
// ============================================================

func TestIntegrationEnumOnDemandRead(t *testing.T) {
	// Deliberately do NOT call LoadSymbols — force on-demand resolution
	conn := setupConnection(t)

	enumName := os.Getenv("ADS_READ_ENUM")
	if enumName == "" {
		enumName = "GVL_ProcessData.eMachineState"
	}

	value, err := conn.ReadFromSymbol(enumName)
	if err != nil {
		t.Skipf("enum symbol %s not available: %v", enumName, err)
	}

	// Value should be numeric (enum ordinal via inferBaseType)
	_, parseErr := strconv.ParseInt(value, 10, 64)
	if parseErr != nil {
		t.Errorf("enum value should be numeric, got %q: %v", value, parseErr)
	}
	t.Logf("on-demand enum: %s = %s", enumName, value)
}

func TestIntegrationDeeplyNestedStruct(t *testing.T) {
	conn := setupConnection(t)
	if err := conn.LoadSymbols(); err != nil {
		t.Fatalf("LoadSymbols failed: %v", err)
	}

	structName := os.Getenv("ADS_READ_DEEP_STRUCT")
	if structName == "" {
		structName = "GVL_ProcessData.stMachineStatus"
	}

	symbols, _ := conn.ListSymbols()
	sym, ok := symbols[structName]
	if !ok {
		t.Skipf("struct %s not found", structName)
	}

	// Measure nesting depth
	maxDepth := 0
	var measureDepth func(s SymbolView, depth int)
	measureDepth = func(s SymbolView, depth int) {
		if depth > maxDepth {
			maxDepth = depth
		}
		for _, child := range s.Children() {
			measureDepth(child, depth+1)
		}
	}
	measureDepth(sym, 0)
	t.Logf("struct %s: depth=%d, direct children=%d, size=%d bytes", structName, maxDepth, len(sym.Children()), sym.Length)

	if maxDepth < 2 {
		t.Skipf("struct only has depth %d, need 2+ levels", maxDepth)
	}

	// Read the struct
	value, err := conn.ReadFromSymbol(structName)
	if err != nil {
		t.Fatalf("ReadFromSymbol(%s) failed: %v", structName, err)
	}

	if len(value) > 200 {
		t.Logf("value (truncated): %.200s...", value)
	} else {
		t.Logf("value: %s", value)
	}

	// Verify all leaf children have values
	var leafCount, emptyCount int
	var checkLeaves func(s SymbolView, path string)
	checkLeaves = func(s SymbolView, path string) {
		if len(s.Children()) == 0 {
			leafCount++
			if s.Value == "" {
				emptyCount++
				t.Errorf("leaf %s has empty value", path)
			}
		} else {
			for name, child := range s.Children() {
				checkLeaves(child, path+"."+name)
			}
		}
	}
	checkLeaves(sym, structName)
	t.Logf("verified %d leaf values, %d empty", leafCount, emptyCount)
}

func TestIntegrationStructMultipleEnumChildren(t *testing.T) {
	conn := setupConnection(t)
	if err := conn.LoadSymbols(); err != nil {
		t.Fatalf("LoadSymbols failed: %v", err)
	}

	structName := os.Getenv("ADS_READ_DEEP_STRUCT")
	if structName == "" {
		structName = "GVL_ProcessData.stMachineStatus"
	}

	symbols, _ := conn.ListSymbols()
	sym, ok := symbols[structName]
	if !ok {
		t.Skipf("struct %s not found", structName)
	}

	// Read the struct to populate values
	_, err := conn.ReadFromSymbol(structName)
	if err != nil {
		t.Fatalf("ReadFromSymbol(%s) failed: %v", structName, err)
	}

	// Find all enum/alias children (non-parseable leaf types)
	type enumInfo struct {
		fullName string
		dataType string
		value    string
	}
	var enums []enumInfo
	var findEnums func(s SymbolView)
	findEnums = func(s SymbolView) {
		for _, child := range s.Children() {
			if !parseableSet[child.DataType] && len(child.Children()) == 0 && child.Length > 0 {
				enums = append(enums, enumInfo{child.FullName, child.DataType, child.Value})
			}
			findEnums(child)
		}
	}
	findEnums(sym)

	if len(enums) < 2 {
		t.Skipf("found %d typed enum children (need 2+) — TC2 uses plain INT for enums, no E_* typed children in structs", len(enums))
	}

	t.Logf("found %d enum children in %s", len(enums), structName)
	for _, e := range enums {
		if e.value == "" {
			t.Errorf("enum child %s (%s) has empty value", e.fullName, e.dataType)
			continue
		}
		_, parseErr := strconv.ParseInt(e.value, 10, 64)
		if parseErr != nil {
			t.Errorf("enum child %s value %q not numeric", e.fullName, e.value)
		}
		t.Logf("  %s (%s) = %s", e.fullName, e.dataType, e.value)
	}
}

// ============================================================
// Transmission mode tests
// ============================================================

func TestIntegrationNotificationServerCycle(t *testing.T) {
	conn := setupConnection(t)
	if err := conn.LoadSymbols(); err != nil {
		t.Fatalf("LoadSymbols failed: %v", err)
	}
	symbols, _ := conn.ListSymbols()
	symbolName := pickParseableSymbol(symbols)
	if symbolName == "" {
		t.Skip("no parseable symbol")
	}

	ch := make(chan *Update, 50)
	handle, err := conn.AddSymbolNotification(symbolName, 100*time.Millisecond, 100*time.Millisecond, TransModeServerCycle, ch)
	if err != nil {
		t.Fatalf("AddSymbolNotification (ServerCycle) failed: %v", err)
	}

	var count int
	timeout := time.After(3 * time.Second)
	for {
		select {
		case u := <-ch:
			count++
			if count == 1 {
				t.Logf("first ServerCycle notification: %s = %s", u.Variable, u.Value)
			}
		case <-timeout:
			goto done
		}
	}
done:
	t.Logf("ServerCycle: received %d notifications in 3s", count)
	if count < 2 {
		t.Errorf("ServerCycle should deliver periodic updates, got %d", count)
	}

	_ = conn.DeleteDeviceNotification(handle)
}

func TestIntegrationNotificationServerCycle2(t *testing.T) {
	conn := setupConnection(t)
	if err := conn.LoadSymbols(); err != nil {
		t.Fatalf("LoadSymbols failed: %v", err)
	}
	symbols, _ := conn.ListSymbols()
	symbolName := pickParseableSymbol(symbols)
	if symbolName == "" {
		t.Skip("no parseable symbol")
	}

	// Check ContextMask before subscribing — determines if InContext or fallback
	sym, err := conn.GetSymbol(symbolName)
	if err != nil {
		t.Fatalf("GetSymbol failed: %v", err)
	}
	if sym.ContextMask == 0 {
		t.Logf("symbol %q has ContextMask=0 (flags=0x%04X) — CyclicInContext will auto-fallback to ServerCycle", symbolName, uint32(sym.Flags))
	} else {
		t.Logf("symbol %q has ContextMask=%d (flags=0x%04X) — CyclicInContext should work natively", symbolName, sym.ContextMask, uint32(sym.Flags))
	}

	ch := make(chan *Update, 50)
	handle, err := conn.AddSymbolNotification(symbolName, 100*time.Millisecond, 100*time.Millisecond, TransModeServerCycle2, ch)
	if err != nil {
		t.Fatalf("AddSymbolNotification failed: %v", err)
	}

	var count int
	timeout := time.After(3 * time.Second)
	for {
		select {
		case u := <-ch:
			count++
			if count == 1 {
				t.Logf("first notification: %s = %s", u.Variable, u.Value)
			}
		case <-timeout:
			goto done
		}
	}
done:
	_ = conn.DeleteDeviceNotification(handle)

	if sym.ContextMask == 0 {
		t.Logf("CyclicInContext fallback to ServerCycle: received %d notifications in 3s", count)
	} else {
		t.Logf("CyclicInContext native: received %d notifications in 3s", count)
	}
	if count == 0 {
		t.Error("expected notifications (with or without fallback), got 0")
	}
}

func TestIntegrationNotificationServerOnChange2(t *testing.T) {
	symbolName := os.Getenv("ADS_WRITE_INT")
	if symbolName == "" {
		t.Skip("ADS_WRITE_INT not set")
	}

	conn := setupConnection(t)
	if err := conn.LoadSymbols(); err != nil {
		t.Fatalf("LoadSymbols failed: %v", err)
	}

	// Check ContextMask — determines if InContext or fallback
	sym, err := conn.GetSymbol(symbolName)
	if err != nil {
		t.Fatalf("GetSymbol failed: %v", err)
	}
	if sym.ContextMask == 0 {
		t.Logf("symbol %q has ContextMask=0 (flags=0x%04X) — OnChangeInContext will auto-fallback to ServerOnChange", symbolName, uint32(sym.Flags))
	} else {
		t.Logf("symbol %q has ContextMask=%d (flags=0x%04X) — OnChangeInContext should work natively", symbolName, sym.ContextMask, uint32(sym.Flags))
	}

	ch := make(chan *Update, 10)
	handle, err := conn.AddSymbolNotification(symbolName, 100*time.Millisecond, 100*time.Millisecond, TransModeServerOnChange2, ch)
	if err != nil {
		t.Fatalf("AddSymbolNotification failed: %v", err)
	}

	// Read original, write a different value to trigger change
	original, _ := conn.ReadFromSymbol(symbolName)
	writeVal := "42"
	if original == "42" {
		writeVal = "100"
	}

	err = conn.WriteToSymbol(symbolName, writeVal)
	if err != nil {
		t.Fatalf("WriteToSymbol failed: %v", err)
	}

	var received bool
	select {
	case update := <-ch:
		if sym.ContextMask == 0 {
			t.Logf("OnChangeInContext fallback to ServerOnChange delivered: %s = %s", update.Variable, update.Value)
		} else {
			t.Logf("OnChangeInContext native delivered: %s = %s", update.Variable, update.Value)
		}
		received = true
	case <-time.After(3 * time.Second):
	}

	// Restore
	_ = conn.WriteToSymbol(symbolName, original)
	_ = conn.DeleteDeviceNotification(handle)
	if !received {
		t.Error("expected notification (with or without fallback) after write, got none")
	}
}

func TestIntegrationNotificationBatchTransModes(t *testing.T) {
	conn := setupConnection(t)
	if err := conn.LoadSymbols(); err != nil {
		t.Fatalf("LoadSymbols failed: %v", err)
	}
	symbols, _ := conn.ListSymbols()
	names := pickParseableSymbols(symbols, 3)
	if len(names) < 3 {
		t.Skip("need at least 3 parseable symbols")
	}

	// Check ContextMask on the symbol that will use CyclicInContext
	sym2, err := conn.GetSymbol(names[2])
	if err != nil {
		t.Fatalf("GetSymbol(%s) failed: %v", names[2], err)
	}
	if sym2.ContextMask == 0 {
		t.Logf("batch CyclicInContext: %s has ContextMask=0 — will auto-fallback to ServerCycle", names[2])
	} else {
		t.Logf("batch CyclicInContext: %s has ContextMask=%d — will use native InContext", names[2], sym2.ContextMask)
	}

	ch := make(chan *Update, 100)
	configs := []NotificationConfig{
		{SymbolName: names[0], MaxDelay: 100 * time.Millisecond, CycleTime: 100 * time.Millisecond, TransmissionMode: TransModeServerCycle},
		{SymbolName: names[1], MaxDelay: 100 * time.Millisecond, CycleTime: 100 * time.Millisecond, TransmissionMode: TransModeServerOnChange},
		{SymbolName: names[2], MaxDelay: 100 * time.Millisecond, CycleTime: 200 * time.Millisecond, TransmissionMode: TransModeServerCycle2},
	}

	_, err = conn.AddSymbolNotifications(configs, ch)
	if err != nil {
		t.Fatalf("AddSymbolNotifications (mixed modes) failed: %v", err)
	}

	// Collect for 3 seconds
	seen := make(map[string]int)
	timeout := time.After(3 * time.Second)
	for {
		select {
		case u := <-ch:
			seen[u.Variable]++
		case <-timeout:
			goto done
		}
	}
done:
	for name, count := range seen {
		t.Logf("  %s: %d notifications", name, count)
	}

	// ServerCycle symbol should have multiple periodic notifications
	if seen[names[0]] < 2 {
		t.Errorf("ServerCycle symbol %s: expected 2+ notifications, got %d", names[0], seen[names[0]])
	}

	// At least the cycle symbols should have notifications
	totalSeen := 0
	for _, c := range seen {
		totalSeen += c
	}
	t.Logf("total: %d notifications across %d symbols", totalSeen, len(seen))

	// Cleanup
	conn.notifs.lock.Lock()
	var handles []uint32
	for h := range conn.notifs.activeNotifications {
		handles = append(handles, h)
	}
	conn.notifs.lock.Unlock()
	_, _ = conn.SumDeleteDeviceNotification(handles)
}

// ============================================================
// Large batch stress tests
// ============================================================

func TestIntegrationLargeBatchRead(t *testing.T) {
	conn := setupConnection(t)
	if err := conn.LoadSymbols(); err != nil {
		t.Fatalf("LoadSymbols failed: %v", err)
	}
	symbols, _ := conn.ListSymbols()
	names := pickParseableSymbols(symbols, 50)
	if len(names) < 10 {
		t.Skipf("only %d parseable symbols, need 10+", len(names))
	}

	// Ensure all symbols have handles before batch read to avoid
	// intermittent failures from handle acquisition under load.
	for _, name := range names {
		if _, err := conn.GetSymbol(name); err != nil {
			t.Fatalf("GetSymbol(%s) failed: %v", name, err)
		}
	}

	t.Logf("batch reading %d symbols", len(names))
	start := time.Now()
	values, err := conn.ReadMultipleSymbols(names)
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("ReadMultipleSymbols failed: %v", err)
	}

	// Log missing and empty symbols for diagnostics.
	// STRING type symbols legitimately hold "" so don't count those as failures.
	var missing, emptyNonString, emptyString []string
	for _, name := range names {
		val, ok := values[name]
		if !ok {
			missing = append(missing, name)
		} else if val == "" {
			if symbols[name].DataType == "STRING" {
				emptyString = append(emptyString, name)
			} else {
				emptyNonString = append(emptyNonString, name)
			}
		}
	}
	t.Logf("batch read %d/%d symbols in %v, %d missing, %d empty (non-string), %d empty strings",
		len(values), len(names), elapsed, len(missing), len(emptyNonString), len(emptyString))
	for _, name := range missing {
		t.Logf("  missing: %s (type=%s)", name, symbols[name].DataType)
	}
	for _, name := range emptyNonString {
		t.Logf("  empty: %s (type=%s)", name, symbols[name].DataType)
	}
	if len(missing) > 0 {
		t.Errorf("%d/%d symbols missing from results", len(missing), len(names))
	}
	if len(emptyNonString) > 0 {
		t.Errorf("%d/%d non-string symbols had empty values", len(emptyNonString), len(names))
	}
}

func TestIntegrationLargeBatchNotification(t *testing.T) {
	conn := setupConnection(t)
	if err := conn.LoadSymbols(); err != nil {
		t.Fatalf("LoadSymbols failed: %v", err)
	}
	symbols, _ := conn.ListSymbols()
	names := pickParseableSymbols(symbols, 20)
	if len(names) < 10 {
		t.Skipf("only %d parseable symbols, need 10+", len(names))
	}

	ch := make(chan *Update, 500)
	var configs []NotificationConfig
	for _, name := range names {
		configs = append(configs, NotificationConfig{
			SymbolName:       name,
			MaxDelay:         200 * time.Millisecond,
			CycleTime:        200 * time.Millisecond,
			TransmissionMode: TransModeServerCycle,
		})
	}

	_, err := conn.AddSymbolNotifications(configs, ch)
	if err != nil {
		t.Fatalf("AddSymbolNotifications (large batch) failed: %v", err)
	}

	// Verify all tracked
	conn.notifs.lock.Lock()
	activeCount := len(conn.notifs.activeNotifications)
	conn.notifs.lock.Unlock()
	if activeCount != len(names) {
		t.Errorf("expected %d active notifications, got %d", len(names), activeCount)
	}

	// Collect for 3 seconds
	seen := make(map[string]bool)
	timeout := time.After(3 * time.Second)
	for {
		select {
		case u := <-ch:
			seen[u.Variable] = true
		case <-timeout:
			goto done
		}
	}
done:
	t.Logf("received notifications for %d/%d symbols in 3s", len(seen), len(names))
	if len(seen) < len(names)/2 {
		t.Errorf("expected notifications from at least %d symbols, got %d", len(names)/2, len(seen))
	}

	// Bulk cleanup
	conn.notifs.lock.Lock()
	var handles []uint32
	for h := range conn.notifs.activeNotifications {
		handles = append(handles, h)
	}
	conn.notifs.lock.Unlock()
	_, _ = conn.SumDeleteDeviceNotification(handles)
}

// TestIntegrationNotificationCycleTimes verifies notifications at different cycle times.
// Tests fast (10ms), standard (100ms), and slow (1000ms) cycles to validate
// the PLC delivers at approximately the requested rate.
func TestIntegrationNotificationCycleTimes(t *testing.T) {
	conn := setupConnection(t)
	if err := conn.LoadSymbols(); err != nil {
		t.Fatalf("LoadSymbols failed: %v", err)
	}
	symbols, _ := conn.ListSymbols()
	symbolName := pickParseableSymbol(symbols)
	if symbolName == "" {
		t.Skip("no parseable symbol")
	}

	tests := []struct {
		name      string
		cycleTime int
		maxDelay  int
		duration  time.Duration
		minCount  int // minimum expected notifications
	}{
		{"fast_10ms", 10, 10, 2 * time.Second, 10},
		{"standard_100ms", 100, 100, 3 * time.Second, 5},
		{"slow_1000ms", 1000, 1000, 5 * time.Second, 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ch := make(chan *Update, 500)
			handle, err := conn.AddSymbolNotification(symbolName, time.Duration(tt.cycleTime)*time.Millisecond, time.Duration(tt.maxDelay)*time.Millisecond, TransModeServerCycle, ch)
			if err != nil {
				t.Fatalf("AddSymbolNotification (cycle=%dms) failed: %v", tt.cycleTime, err)
			}

			var count int
			timeout := time.After(tt.duration)
			for {
				select {
				case <-ch:
					count++
				case <-timeout:
					goto done
				}
			}
		done:
			_ = conn.DeleteDeviceNotification(handle)
			t.Logf("cycle=%dms: received %d notifications in %v", tt.cycleTime, count, tt.duration)
			if count < tt.minCount {
				t.Errorf("expected at least %d notifications, got %d", tt.minCount, count)
			}
		})
	}
}

// TestIntegrationNotificationMaxDelay verifies the maxDelay parameter affects
// notification batching. With a long maxDelay and short cycleTime, the PLC
// may batch multiple changes before sending.
func TestIntegrationNotificationMaxDelay(t *testing.T) {
	conn := setupConnection(t)
	if err := conn.LoadSymbols(); err != nil {
		t.Fatalf("LoadSymbols failed: %v", err)
	}
	symbols, _ := conn.ListSymbols()
	symbolName := pickParseableSymbol(symbols)
	if symbolName == "" {
		t.Skip("no parseable symbol")
	}

	// Short cycle, long maxDelay — PLC may batch notifications
	ch := make(chan *Update, 100)
	handle, err := conn.AddSymbolNotification(symbolName, 50*time.Millisecond, 2000*time.Millisecond, TransModeServerCycle, ch)
	if err != nil {
		t.Fatalf("AddSymbolNotification failed: %v", err)
	}

	var count int
	timeout := time.After(5 * time.Second)
	for {
		select {
		case <-ch:
			count++
		case <-timeout:
			goto done
		}
	}
done:
	_ = conn.DeleteDeviceNotification(handle)
	t.Logf("cycle=50ms, maxDelay=2000ms: received %d notifications in 5s", count)
	// Should still receive notifications, just possibly batched
	if count == 0 {
		t.Error("expected notifications with maxDelay=2000ms, got 0")
	}
}

// TestIntegrationNotificationZeroMaxDelay verifies maxDelay=0 delivers
// notifications as fast as possible (no batching).
func TestIntegrationNotificationZeroMaxDelay(t *testing.T) {
	conn := setupConnection(t)
	if err := conn.LoadSymbols(); err != nil {
		t.Fatalf("LoadSymbols failed: %v", err)
	}
	symbols, _ := conn.ListSymbols()
	symbolName := pickParseableSymbol(symbols)
	if symbolName == "" {
		t.Skip("no parseable symbol")
	}

	ch := make(chan *Update, 500)
	handle, err := conn.AddSymbolNotification(symbolName, 100*time.Millisecond, 0*time.Millisecond, TransModeServerCycle, ch)
	if err != nil {
		t.Fatalf("AddSymbolNotification (maxDelay=0) failed: %v", err)
	}

	var count int
	timeout := time.After(3 * time.Second)
	for {
		select {
		case <-ch:
			count++
		case <-timeout:
			goto done
		}
	}
done:
	_ = conn.DeleteDeviceNotification(handle)
	t.Logf("maxDelay=0: received %d notifications in 3s", count)
	if count < 2 {
		t.Errorf("expected notifications with maxDelay=0, got %d", count)
	}
}

// TestIntegrationReadAllParseableTypes loads all symbols, groups them by datatype,
// and reads at least one symbol of each parseable type. Validates that the library
// can parse every supported ADS datatype from real PLC data.
func TestIntegrationReadAllParseableTypes(t *testing.T) {
	conn := setupConnection(t)
	if err := conn.LoadSymbols(); err != nil {
		t.Fatalf("LoadSymbols failed: %v", err)
	}
	symbols, _ := conn.ListSymbols()

	// Group symbols by base datatype
	byType := make(map[string]string) // datatype → first symbol name
	for name, sym := range symbols {
		if parseableSet[sym.DataType] {
			if _, exists := byType[sym.DataType]; !exists {
				byType[sym.DataType] = name
			}
		}
	}

	t.Logf("found %d parseable types on PLC", len(byType))
	for dt, name := range byType {
		t.Run(dt, func(t *testing.T) {
			val, err := conn.ReadFromSymbol(name)
			if err != nil {
				t.Fatalf("ReadFromSymbol(%s) [type=%s] failed: %v", name, dt, err)
			}
			if val == "" && dt != "STRING" {
				t.Errorf("ReadFromSymbol(%s) [type=%s] returned empty", name, dt)
			}
			t.Logf("%s (%s) = %q", name, dt, val)
		})
	}

	// Log which parseable types were NOT found on PLC
	for dt := range parseableSet {
		if _, found := byType[dt]; !found {
			t.Logf("type %s: not present on PLC (no coverage)", dt)
		}
	}
}


// TestIntegrationRapidSubscribeUnsubscribe subscribes and unsubscribes quickly
// in a loop to verify handle management under rapid lifecycle churn.
func TestIntegrationRapidSubscribeUnsubscribe(t *testing.T) {
	conn := setupConnection(t)
	if err := conn.LoadSymbols(); err != nil {
		t.Fatalf("LoadSymbols failed: %v", err)
	}
	symbols, _ := conn.ListSymbols()
	symbolName := pickParseableSymbol(symbols)
	if symbolName == "" {
		t.Skip("no parseable symbol")
	}

	const iterations = 10
	for i := 0; i < iterations; i++ {
		ch := make(chan *Update, 5)
		handle, err := conn.AddSymbolNotification(symbolName, 100*time.Millisecond, 100*time.Millisecond, TransModeServerCycle, ch)
		if err != nil {
			t.Fatalf("iteration %d: AddSymbolNotification failed: %v", i, err)
		}
		err = conn.DeleteDeviceNotification(handle)
		if err != nil {
			t.Fatalf("iteration %d: DeleteDeviceNotification failed: %v", i, err)
		}
	}

	// Verify clean state after rapid churn
	conn.notifs.lock.Lock()
	remaining := len(conn.notifs.activeNotifications)
	conn.notifs.lock.Unlock()
	if remaining != 0 {
		t.Errorf("expected 0 active notifications after %d cycles, got %d", iterations, remaining)
	}
	t.Logf("completed %d rapid subscribe/unsubscribe cycles, 0 leaked handles", iterations)
}

// TestIntegrationDockerRoute validates route registration and ADS communication
// from a Docker container. When ADS_HOST_IP is set, verifies the connection
// uses it as callback IP. Always verifies ReadDeviceInfo + LoadSymbols work.
func TestIntegrationDockerRoute(t *testing.T) {
	conn := setupConnection(t)

	t.Logf("source NetID: %d.%d.%d.%d.%d.%d",
		conn.source.NetID[0], conn.source.NetID[1],
		conn.source.NetID[2], conn.source.NetID[3],
		conn.source.NetID[4], conn.source.NetID[5])
	t.Logf("callbackIP: %q", conn.callbackIP)

	hostIP := os.Getenv("ADS_HOST_IP")
	if hostIP != "" && conn.callbackIP != hostIP {
		t.Errorf("callbackIP=%q, want ADS_HOST_IP=%q", conn.callbackIP, hostIP)
	}

	// Verify connection works (proves route is valid)
	info, err := conn.ReadDeviceInfo()
	if err != nil {
		t.Fatalf("ReadDeviceInfo failed: %v", err)
	}
	t.Logf("device: %s v%d.%d.%d", string(info.DeviceName[:]), info.Major, info.Minor, info.Version)

	if err := conn.LoadSymbols(); err != nil {
		t.Fatalf("LoadSymbols failed: %v", err)
	}
	symbols, _ := conn.ListSymbols()
	t.Logf("loaded %d symbols", len(symbols))
}

func TestIntegrationWSTRING(t *testing.T) {
	conn := setupConnection(t)

	if err := conn.LoadSymbols(); err != nil {
		t.Fatalf("LoadSymbols failed: %v", err)
	}

	symbols, err := conn.ListSymbols()
	if err != nil {
		t.Fatalf("ListSymbols failed: %v", err)
	}

	// Find a WSTRING symbol
	var wstringName string
	for name, sym := range symbols {
		if sym.DataType == "WSTRING" {
			wstringName = name
			break
		}
	}
	if wstringName == "" {
		t.Skip("no WSTRING symbol found on PLC")
	}

	t.Logf("found WSTRING symbol: %s", wstringName)

	// Read current value
	val, err := conn.ReadFromSymbol(wstringName)
	if err != nil {
		t.Fatalf("ReadFromSymbol(%s) failed: %v", wstringName, err)
	}
	t.Logf("current value: %q", val)

	// Write a test value and read back
	testVal := "WTest"
	if err := conn.WriteToSymbol(wstringName, testVal); err != nil {
		t.Logf("WriteToSymbol(%s) failed (may be read-only): %v", wstringName, err)
		return
	}

	readBack, err := conn.ReadFromSymbol(wstringName)
	if err != nil {
		t.Fatalf("ReadFromSymbol after write failed: %v", err)
	}
	if readBack != testVal {
		t.Errorf("got %q, want %q", readBack, testVal)
	}

	// Restore original value
	if val != "" {
		_ = conn.WriteToSymbol(wstringName, val)
	}
}

func TestIntegrationBitSymbol(t *testing.T) {
	conn := setupConnection(t)

	if err := conn.LoadSymbols(); err != nil {
		t.Fatalf("LoadSymbols failed: %v", err)
	}

	symbols, err := conn.ListSymbols()
	if err != nil {
		t.Fatalf("ListSymbols failed: %v", err)
	}

	// Find a symbol with BitValue flag
	var bitName string
	for name, sym := range symbols {
		if sym.Flags.Has(SymbolFlagBitValue) {
			bitName = name
			break
		}
	}
	if bitName == "" {
		t.Skip("no BitValue symbol found on PLC")
	}

	t.Logf("found BitValue symbol: %s (flags=0x%04X)", bitName, uint32(symbols[bitName].Flags))

	// Read should return "true" or "false"
	val, err := conn.ReadFromSymbol(bitName)
	if err != nil {
		t.Fatalf("ReadFromSymbol(%s) failed: %v", bitName, err)
	}
	if val != "true" && val != "false" {
		t.Errorf("expected 'true' or 'false', got %q", val)
	}
	t.Logf("value: %s", val)
}

func TestIntegrationReadProcessInputSize(t *testing.T) {
	conn := setupConnection(t)

	size, err := conn.ReadProcessInputSize()
	if err != nil {
		if strings.Contains(err.Error(), "service not supported") {
			t.Skip("PLC has no physical I/O configured (process image unavailable)")
		}
		t.Fatalf("ReadProcessInputSize failed: %v", err)
	}
	if size == 0 {
		t.Error("input image size is 0")
	}
	t.Logf("process input image size: %d bytes", size)
}

func TestIntegrationReadProcessInput(t *testing.T) {
	conn := setupConnection(t)

	// Read first 4 bytes of input image
	data, err := conn.ReadProcessInput(0, 4)
	if err != nil {
		if strings.Contains(err.Error(), "service not supported") {
			t.Skip("PLC has no physical I/O configured (process image unavailable)")
		}
		t.Fatalf("ReadProcessInput failed: %v", err)
	}
	if len(data) != 4 {
		t.Errorf("expected 4 bytes, got %d", len(data))
	}
	t.Logf("first 4 input bytes: %02X", data)
}

func TestIntegrationReadProcessOutput(t *testing.T) {
	conn := setupConnection(t)

	// Read first 4 bytes of output image
	data, err := conn.ReadProcessOutput(0, 4)
	if err != nil {
		if strings.Contains(err.Error(), "service not supported") {
			t.Skip("PLC has no physical I/O configured (process image unavailable)")
		}
		t.Fatalf("ReadProcessOutput failed: %v", err)
	}
	if len(data) != 4 {
		t.Errorf("expected 4 bytes, got %d", len(data))
	}
	t.Logf("first 4 output bytes: %02X", data)
}

func TestIntegrationReadProcessInputBit(t *testing.T) {
	conn := setupConnection(t)

	// Read bit 0 of first input byte
	val, err := conn.ReadProcessInputBit(0, 0)
	if err != nil {
		if strings.Contains(err.Error(), "service not supported") {
			t.Skip("PLC has no physical I/O configured (process image unavailable)")
		}
		t.Fatalf("ReadProcessInputBit failed: %v", err)
	}
	t.Logf("input bit 0.0 = %v", val)
}
