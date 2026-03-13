//go:build integration

package ads

import (
	"context"
	"os"
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
//   ADS_LOCAL_AMS    - Local AMS NetID (default: auto)
//   ADS_SYMBOL_NAME  - Symbol to read (default: first found)

func getEnvOrDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func setupConnection(t *testing.T) *Connection {
	t.Helper()

	ip := getEnvOrDefault("ADS_PLC_IP", "192.168.3.224")
	targetAMS := getEnvOrDefault("ADS_TARGET_AMS", "5.154.236.19.1.1")
	localAMS := getEnvOrDefault("ADS_LOCAL_AMS", "auto")

	conn, err := NewConnection(context.Background(), ip, 48898, targetAMS, 851, localAMS, 10500, 5*time.Second)
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
		for name := range symbols {
			symbolName = name
			break
		}
	}
	if symbolName == "" {
		t.Skip("no symbols available")
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
		for name := range symbols {
			symbolName = name
			break
		}
	}
	if symbolName == "" {
		t.Skip("no symbols available")
	}

	ch := make(chan *Update, 10)
	handle, err := conn.AddSymbolNotification(symbolName, 100, 100, TransModeServerOnChange, ch)
	if err != nil {
		t.Fatalf("AddSymbolNotification(%q) failed: %v", symbolName, err)
	}
	t.Logf("notification handle: %d", handle)

	select {
	case update := <-ch:
		t.Logf("notification: %s = %s at %v", update.Variable, update.Value, update.TimeStamp)
	case <-time.After(5 * time.Second):
		t.Log("no notification received within 5s (may be expected if value doesn't change)")
	}
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
		for name := range symbols {
			symbolName = name
			break
		}
	}
	if symbolName == "" {
		t.Skip("no symbols available")
	}

	ch := make(chan *Update, 10)

	// Subscribe
	handle, err := conn.AddSymbolNotification(symbolName, 100, 100, TransModeServerOnChange, ch)
	if err != nil {
		t.Fatalf("AddSymbolNotification(%q) failed: %v", symbolName, err)
	}
	t.Logf("subscribed to %s (handle=%d)", symbolName, handle)

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

	// Verify no more notifications arrive after unsubscribe
	select {
	case update := <-ch:
		t.Logf("warning: received notification after unsubscribe: %s = %s (may be in-flight)", update.Variable, update.Value)
	case <-time.After(2 * time.Second):
		t.Log("no notification after unsubscribe (expected)")
	}
}
