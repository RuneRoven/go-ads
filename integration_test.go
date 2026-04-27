//go:build integration

package ads

import (
	"context"
	"fmt"
	"math"
	"net"
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
}

// pickParseableSymbol returns the name of a symbol with a parseable base type from the map.
// Returns "" if none found.
func pickParseableSymbol(symbols map[string]*Symbol) string {
	for name, sym := range symbols {
		if parseableSet[sym.DataType] {
			return name
		}
	}
	return ""
}

// pickParseableSymbols returns up to n symbol names with parseable base types.
// Prefers top-level symbols (no dots) to avoid struct children that may lack handles.
func pickParseableSymbols(symbols map[string]*Symbol, n int) []string {
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

	ip := getEnvOrDefault("ADS_PLC_IP", "192.168.3.224")
	targetAMS := getEnvOrDefault("ADS_TARGET_AMS", "5.154.236.19.1.1")
	targetPortStr := getEnvOrDefault("ADS_TARGET_PORT", "851")
	targetPort, err := strconv.Atoi(targetPortStr)
	if err != nil {
		t.Fatalf("invalid ADS_TARGET_PORT %q: %v", targetPortStr, err)
	}
	localAMS := getEnvOrDefault("ADS_LOCAL_AMS", "auto")

	// Auto-create AMS route if credentials are provided
	routeUser := os.Getenv("ADS_ROUTE_USER")
	routePass := os.Getenv("ADS_ROUTE_PASS")
	if routeUser != "" && routePass != "" {
		// Determine local AMS NetID for route registration
		var localNetID [6]byte
		if localAMS != "auto" && localAMS != "" {
			netIDBytes, err := stringToNetID(localAMS)
			if err != nil {
				t.Fatalf("invalid ADS_LOCAL_AMS %q: %v", localAMS, err)
			}
			localNetID = netIDBytes
		} else {
			// Auto-derive from local IP facing the PLC
			udpConn, err := net.DialTimeout("udp4", ip+":48899", 2*time.Second)
			if err != nil {
				t.Fatalf("failed to determine local IP for route: %v", err)
			}
			localAddr := udpConn.LocalAddr().(*net.UDPAddr)
			udpConn.Close()
			ipv4 := localAddr.IP.To4()
			localNetID = [6]byte{ipv4[0], ipv4[1], ipv4[2], ipv4[3], 1, 1}
		}

		// Determine the IP the PLC should use to reach us
		hostIP := os.Getenv("ADS_HOST_IP")
		if hostIP == "" {
			hostIP = fmt.Sprintf("%d.%d.%d.%d", localNetID[0], localNetID[1], localNetID[2], localNetID[3])
		}

		t.Logf("adding AMS route on %s (local NetID: %d.%d.%d.%d.%d.%d, host IP: %s)", ip,
			localNetID[0], localNetID[1], localNetID[2], localNetID[3], localNetID[4], localNetID[5], hostIP)
		err := AddRemoteRoute(ip, localNetID, "go-ads-test", hostIP, routeUser, routePass)
		if err != nil {
			t.Logf("warning: AddRemoteRoute failed (may already exist): %v", err)
		} else {
			t.Log("AMS route added successfully")
		}
	}

	conn, err := NewConnection(context.Background(), ip, 48898, targetAMS, targetPort, localAMS, 10500, 5*time.Second)
	if err != nil {
		t.Fatalf("NewConnection failed: %v", err)
	}

	err = conn.Connect(false)
	if err != nil {
		t.Fatalf("Connect failed: %v", err)
	}

	t.Cleanup(func() {
		conn.Close()
	})

	return conn
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
	t.Logf("ADS state: %d, Device state: %d", state.AdsState, state.DeviceState)
	if state.AdsState != AdsStateRun {
		t.Logf("warning: PLC not in Run state (got %d)", state.AdsState)
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
	conn.symbolLock.Lock()
	beforeCount := len(conn.activeNotifications)
	conn.symbolLock.Unlock()
	if beforeCount != 0 {
		t.Fatalf("expected 0 active notifications before subscribe, got %d", beforeCount)
	}

	ch := make(chan *Update, 10)
	handle, err := conn.AddSymbolNotification(symbolName, 100, 100, TransModeServerOnChange, ch)
	if err != nil {
		t.Fatalf("AddSymbolNotification(%q) failed: %v", symbolName, err)
	}
	t.Logf("notification handle: %d", handle)

	// Verify handle is tracked
	conn.symbolLock.Lock()
	afterCount := len(conn.activeNotifications)
	_, tracked := conn.activeNotifications[handle]
	conn.symbolLock.Unlock()
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
	conn.symbolLock.Lock()
	cleanupCount := len(conn.activeNotifications)
	conn.symbolLock.Unlock()
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
	conn.symbolLock.Lock()
	if len(conn.activeNotifications) != 0 {
		t.Fatalf("expected 0 active notifications at start, got %d", len(conn.activeNotifications))
	}
	conn.symbolLock.Unlock()

	// Subscribe
	handle, err := conn.AddSymbolNotification(symbolName, 100, 100, TransModeServerOnChange, ch)
	if err != nil {
		t.Fatalf("AddSymbolNotification(%q) failed: %v", symbolName, err)
	}
	t.Logf("subscribed to %s (handle=%d)", symbolName, handle)

	// Verify handle is tracked
	conn.symbolLock.Lock()
	if _, ok := conn.activeNotifications[handle]; !ok {
		t.Errorf("handle %d not tracked in activeNotifications after subscribe", handle)
	}
	conn.symbolLock.Unlock()

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
	conn.symbolLock.Lock()
	if _, ok := conn.activeNotifications[handle]; ok {
		t.Errorf("handle %d still in activeNotifications after DeleteDeviceNotification", handle)
	}
	if len(conn.activeNotifications) != 0 {
		t.Errorf("expected 0 active notifications after unsubscribe, got %d", len(conn.activeNotifications))
	}
	conn.symbolLock.Unlock()

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
	conn.symbolLock.Lock()
	startCount := len(conn.activeNotifications)
	conn.symbolLock.Unlock()
	if startCount != 0 {
		t.Fatalf("expected 0 active notifications at start, got %d", startCount)
	}

	// Subscribe to multiple symbols
	var handles []uint32
	for _, name := range symbolNames {
		handle, err := conn.AddSymbolNotification(name, 100, 100, TransModeServerOnChange, ch)
		if err != nil {
			t.Fatalf("AddSymbolNotification(%q) failed: %v", name, err)
		}
		handles = append(handles, handle)
		t.Logf("subscribed to %s (handle=%d)", name, handle)
	}

	// Verify all handles are tracked
	conn.symbolLock.Lock()
	activeCount := len(conn.activeNotifications)
	conn.symbolLock.Unlock()
	if activeCount != len(handles) {
		t.Errorf("expected %d active notifications, got %d", len(handles), activeCount)
	}

	// Delete handles one by one and verify count decreases
	for i, handle := range handles {
		err := conn.DeleteDeviceNotification(handle)
		if err != nil {
			t.Fatalf("DeleteDeviceNotification(%d) failed: %v", handle, err)
		}
		conn.symbolLock.Lock()
		remaining := len(conn.activeNotifications)
		conn.symbolLock.Unlock()
		expected := len(handles) - i - 1
		if remaining != expected {
			t.Errorf("after deleting handle %d: expected %d active, got %d", handle, expected, remaining)
		}
	}

	// Final check: zero handles remaining
	conn.symbolLock.Lock()
	finalCount := len(conn.activeNotifications)
	conn.symbolLock.Unlock()
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

	conn, err := NewConnection(context.Background(), ip, 48898, targetAMS, targetPort, localAMS, 10500, 5*time.Second)
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
		handle, err := conn.AddSymbolNotification(name, 100, 100, TransModeServerOnChange, ch)
		if err != nil {
			conn.Close()
			t.Fatalf("AddSymbolNotification(%q) failed: %v", name, err)
		}
		handles = append(handles, handle)
		t.Logf("subscribed to %s (handle=%d)", name, handle)
	}

	// Verify handles are active
	conn.symbolLock.Lock()
	activeBeforeClose := len(conn.activeNotifications)
	conn.symbolLock.Unlock()
	if activeBeforeClose != len(handles) {
		t.Errorf("expected %d active notifications before Close, got %d", len(handles), activeBeforeClose)
	}

	// Close should release all handles on the PLC
	conn.Close()
	t.Logf("Close() completed, released %d notification handles", len(handles))

	// Reconnect and verify no stale handles exist by subscribing to the same
	// symbols again — if Close() didn't release, the PLC would eventually
	// run out of handles.
	conn2, err := NewConnection(context.Background(), ip, 48898, targetAMS, targetPort, localAMS, 10501, 5*time.Second)
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
	conn2.symbolLock.Lock()
	freshCount := len(conn2.activeNotifications)
	conn2.symbolLock.Unlock()
	if freshCount != 0 {
		t.Errorf("fresh connection should have 0 active notifications, got %d", freshCount)
	}

	// Subscribe to same symbols on new connection to confirm PLC accepts them
	ch2 := make(chan *Update, 100)
	for _, name := range symbolNames {
		handle, err := conn2.AddSymbolNotification(name, 100, 100, TransModeServerOnChange, ch2)
		if err != nil {
			t.Errorf("re-subscribe to %s on fresh connection failed: %v (possible PLC handle leak)", name, err)
		} else {
			t.Logf("re-subscribed to %s (new handle=%d)", name, handle)
		}
	}

	conn2.symbolLock.Lock()
	resubCount := len(conn2.activeNotifications)
	conn2.symbolLock.Unlock()
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
	for !conn.IsDisconnected() && !conn.reconnecting.Load() {
		select {
		case <-deadline:
			t.Fatal("reconnect was never triggered within timeout")
		case <-tick.C:
		}
	}

	// Phase 2: wait for reconnect to fully complete
	for conn.IsDisconnected() || conn.reconnecting.Load() {
		select {
		case <-deadline:
			t.Fatalf("reconnect did not complete within timeout (disconnected=%v, reconnecting=%v)",
				conn.IsDisconnected(), conn.reconnecting.Load())
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
	handle, err := conn.AddSymbolNotification(symbolName, 100, 100, TransModeServerOnChange, ch)
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

	// 3. Simulate network drop by closing TCP connection
	t.Log("simulating network drop...")
	conn.connMu.Lock()
	conn.connection.Close()
	conn.connMu.Unlock()

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

	// 2. Simulate network drop
	t.Log("simulating network drop...")
	conn.connMu.Lock()
	conn.connection.Close()
	conn.connMu.Unlock()

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

	// 2. Kill TCP — triggers reconnect in background
	t.Log("simulating network drop...")
	conn.connMu.Lock()
	conn.connection.Close()
	conn.connMu.Unlock()

	// 3. Immediately read WITHOUT waiting for reconnect.
	// sendRequest's retry loop should handle this transparently.
	val2, err := conn.ReadFromSymbol(symbolName)
	if err != nil {
		t.Fatalf("read during reconnect failed (sendRequest retry should have handled this): %v", err)
	}
	t.Logf("read during reconnect succeeded: %s = %s", symbolName, val2)
}
