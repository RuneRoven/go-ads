//go:build integration

package ads

import (
	"fmt"
	"os"
	"sort"
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
//
// Validates: R-CACHE-001, R-VIEW-005.
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
	info, err := conn.client.Load().ReadDeviceInfo()
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
	buf.WriteString(fmt.Sprintf("(* symbol dump from %s — %s v%d.%d.%d *)\n",
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

func browseSetupConnection(t *testing.T) *Session {
	t.Helper()
	return setupConnectionWithDefaults(t, connDefaults{
		ip:        "192.168.0.1",
		targetAMS: "5.0.0.1.1.1",
		routeName: "go-ads-browse",
	})
}
