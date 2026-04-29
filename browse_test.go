//go:build integration

package ads

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
)

// TestBrowseAllSymbols connects to the PLC, loads all symbols slowly (chunked),
// and writes a .var file listing every symbol with its type, size, group, and offset.
//
// Run for each PLC:
//
//	set -a && source .env.integration && set +a && go test -tags integration -run TestBrowseAllSymbols -v -timeout 60s
func TestBrowseAllSymbols(t *testing.T) {
	conn := browseSetupConnection(t)

	ip := getEnvOrDefault("ADS_PLC_IP", "192.168.0.1")

	// Load all symbols + datatypes slowly to avoid disrupting PLC real-time tasks.
	err := conn.LoadSymbolsSlow(SlowDiscoveryConfig{
		ChunkSize:  4096,
		ChunkDelay: 100 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("LoadSymbolsSlow failed: %v", err)
	}

	symbols, err := conn.ListSymbols()
	if err != nil {
		t.Fatalf("ListSymbols failed: %v", err)
	}

	t.Logf("Loaded %d symbols from %s", len(symbols), ip)

	// Get device info for the header.
	info, err := conn.ReadDeviceInfo()
	if err != nil {
		t.Fatalf("ReadDeviceInfo failed: %v", err)
	}

	// Collect and sort symbol names for deterministic output.
	names := make([]string, 0, len(symbols))
	for name := range symbols {
		names = append(names, name)
	}
	sort.Strings(names)

	// Build .var file content.
	var buf strings.Builder
	buf.WriteString(fmt.Sprintf("(* Symbol dump from %s — %s v%d.%d.%d *)\n",
		ip, info.DeviceName, info.Major, info.Minor, info.Version))
	buf.WriteString(fmt.Sprintf("(* %d symbols, generated %s *)\n\n",
		len(symbols), time.Now().Format("2006-01-02 15:04:05")))

	// Header line
	buf.WriteString(fmt.Sprintf("%-60s %-20s %6s  %10s  %10s  %s\n",
		"Name", "DataType", "Size", "Group", "Offset", "Comment"))
	buf.WriteString(strings.Repeat("-", 120) + "\n")

	for _, name := range names {
		sym := symbols[name]
		comment := ""
		if sym.Comment != "" {
			comment = sym.Comment
		}
		buf.WriteString(fmt.Sprintf("%-60s %-20s %6d  0x%08X  0x%08X  %s\n",
			sym.FullName, sym.DataType, sym.Length, sym.Group, sym.Offset, comment))
	}

	filename := fmt.Sprintf("plc_%s.var", strings.ReplaceAll(ip, ".", "_"))
	err = os.WriteFile(filename, []byte(buf.String()), 0o644)
	if err != nil {
		t.Fatalf("failed to write %s: %v", filename, err)
	}

	t.Logf("Wrote %s (%d symbols)", filename, len(symbols))
}

// browseSetupConnection is a minimal copy of setupConnection for browse_test.go
// to avoid coupling with integration_test.go helpers.
func browseSetupConnection(t *testing.T) *Connection {
	t.Helper()

	ip := getEnvOrDefault("ADS_PLC_IP", "192.168.0.1")
	targetAMS := getEnvOrDefault("ADS_TARGET_AMS", "5.0.0.1.1.1")
	targetPortStr := getEnvOrDefault("ADS_TARGET_PORT", "851")
	targetPort, err := strconv.Atoi(targetPortStr)
	if err != nil {
		t.Fatalf("invalid ADS_TARGET_PORT %q: %v", targetPortStr, err)
	}
	localAMS := getEnvOrDefault("ADS_LOCAL_AMS", "auto")

	// Build connection options — WithRoute registers route BEFORE any ADS commands
	var opts []ConnectionOption
	hostIP := os.Getenv("ADS_HOST_IP")
	if hostIP != "" {
		opts = append(opts, WithHostIP(hostIP))
	}
	routeUser := os.Getenv("ADS_ROUTE_USER")
	routePass := os.Getenv("ADS_ROUTE_PASS")
	if routeUser != "" && routePass != "" {
		opts = append(opts, WithRoute("go-ads-browse", routeUser, routePass))
	}

	conn, err := NewConnection(context.Background(), ip, 48898, targetAMS, targetPort, localAMS, 10500, 5*time.Second, opts...)
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
