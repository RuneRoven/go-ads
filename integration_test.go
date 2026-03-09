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
	t.Logf("Device: %s (version %d.%d.%d)", info.DeviceName, info.MajorVersion, info.MinorVersion, info.VersionBuild)
}

func TestIntegrationReadState(t *testing.T) {
	conn := setupConnection(t)
	adsState, deviceState, err := conn.ReadState()
	if err != nil {
		t.Fatalf("ReadState failed: %v", err)
	}
	t.Logf("ADS state: %d, Device state: %d", adsState, deviceState)
	if adsState != AdsStateRun {
		t.Logf("warning: PLC not in Run state (got %d)", adsState)
	}
}

func TestIntegrationListSymbols(t *testing.T) {
	conn := setupConnection(t)
	symbols := conn.ListSymbols()
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
		// Pick the first symbol from the list
		symbols := conn.ListSymbols()
		for name := range symbols {
			symbolName = name
			break
		}
	}
	if symbolName == "" {
		t.Skip("no symbols available")
	}

	value, err := conn.ReadFromSymbol(symbolName)
	if err != nil {
		t.Fatalf("ReadFromSymbol(%q) failed: %v", symbolName, err)
	}
	t.Logf("%s = %s", symbolName, value)
}

func TestIntegrationNotification(t *testing.T) {
	conn := setupConnection(t)
	symbolName := os.Getenv("ADS_SYMBOL_NAME")
	if symbolName == "" {
		symbols := conn.ListSymbols()
		for name := range symbols {
			symbolName = name
			break
		}
	}
	if symbolName == "" {
		t.Skip("no symbols available")
	}

	ch := make(chan *Update, 10)
	err := conn.AddSymbolNotification(symbolName, 100, 100, TransModeServerOnChange, ch)
	if err != nil {
		t.Fatalf("AddSymbolNotification(%q) failed: %v", symbolName, err)
	}

	select {
	case update := <-ch:
		t.Logf("notification: %s = %s at %v", update.Variable, update.Value, update.TimeStamp)
	case <-time.After(5 * time.Second):
		t.Log("no notification received within 5s (may be expected if value doesn't change)")
	}
}
