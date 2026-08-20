//go:build integration

package ads

import (
	"context"
	"log/slog"
	"os"
	"strconv"
	"testing"
	"time"
)

// notification_scale_integration_test.go — batch-notification scale test on
// real hardware.
//
// The existing hardware notification tests subscribe 3 symbols one at a time,
// which is why the v2.2.0 subscribe-race regression shipped unnoticed: it only
// bites on the BATCH path (AddSymbolNotifications), on a PLC that answers
// 0x0701 to the sum command so the batch degrades to one Add per symbol, and
// only once the batch runs longer than the race window. Measured on TC2 before
// the fix: 3→3, 10→10, 11→11, 12→10, 40→10 tags delivered.
//
// Run against TwinCAT 2 (the sum-unsupported case) with:
//
//	set -a && . ./.env.integration.70 && set +a && \
//	  go test -v -tags integration -timeout 10m -run TestIntegrationNotificationBatchScale .
//
// ADS_NOTIF_SCALE overrides the symbol count (default 40).

// TestIntegrationNotificationBatchScale batch-subscribes N symbols and
// requires every one of them to deliver at least one sample. A handle reaped
// mid-registration never streams, so a missing symbol is the regression.
func TestIntegrationNotificationBatchScale(t *testing.T) {
	want := 40
	if v := os.Getenv("ADS_NOTIF_SCALE"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			t.Fatalf("invalid ADS_NOTIF_SCALE %q: %v", v, err)
		}
		want = n
	}

	logs := &testLogHandler{}
	conn := setupConnectionWithDefaults(t, connDefaults{
		ip:        "192.168.3.224",
		targetAMS: "5.154.236.19.1.1",
		routeName: "go-ads-test",
	}, WithLogger(slog.New(logs)))

	if err := conn.LoadSymbols(context.Background()); err != nil {
		t.Fatalf("LoadSymbols failed: %v", err)
	}
	symbols, err := conn.ListSymbols()
	if err != nil {
		t.Fatalf("ListSymbols failed: %v", err)
	}
	names := pickParseableSymbols(symbols, want)
	if len(names) < 2 {
		t.Skipf("need at least 2 parseable symbols, PLC offers %d", len(names))
	}
	if len(names) < want {
		t.Logf("PLC offers %d parseable symbols, requested %d — running with %d", len(names), want, len(names))
	}

	configs := make([]NotificationConfig, len(names))
	for i, name := range names {
		configs[i] = NotificationConfig{
			SymbolName:       name,
			MaxDelay:         100 * time.Millisecond,
			CycleTime:        100 * time.Millisecond,
			TransmissionMode: TransModeServerOnChange,
		}
	}

	// Buffer generously: a dropped Update would look like a reaped handle.
	ch := make(chan *Update, 16*len(configs))
	results, err := conn.AddSymbolNotifications(context.Background(), configs, ch)
	if err != nil {
		t.Fatalf("AddSymbolNotifications(%d symbols) failed: %v", len(configs), err)
	}
	subscribed := make(map[string]bool, len(results))
	for i, r := range results {
		switch {
		case r.Skipped != nil:
			t.Errorf("subscribe skipped for %s: %v", names[i], r.Skipped)
		case r.Error != ReturnCodeNoErrors:
			t.Errorf("PLC rejected %s: 0x%04X (%v)", names[i], uint32(r.Error), r.Error)
		default:
			subscribed[names[i]] = true
		}
	}
	if len(subscribed) == 0 {
		t.Fatal("no symbol subscribed successfully")
	}
	t.Logf("subscribed %d/%d symbols", len(subscribed), len(configs))

	// TransModeServerOnChange delivers a first sample at subscribe even for a
	// constant, so every subscribed symbol must report inside this window
	// regardless of whether its value moves.
	const collectFor = 15 * time.Second
	seen := make(map[string]string, len(subscribed))
	deadline := time.After(collectFor)
collect:
	for len(seen) < len(subscribed) {
		select {
		case u := <-ch:
			if _, first := seen[u.Variable]; !first {
				seen[u.Variable] = u.Value
			}
		case <-deadline:
			break collect
		}
	}

	t.Logf("delivered %d/%d subscribed symbols in %v", len(seen), len(subscribed), collectFor)
	if len(seen) != len(subscribed) {
		for name := range subscribed {
			if _, ok := seen[name]; !ok {
				t.Errorf("no sample for %s — handle registered but never streamed", name)
			}
		}
	}

	// The reaper firing on our own handles is the specific regression; catch it
	// even in the (unexpected) case where every symbol still got through.
	if rec := logs.findByMessage("orphan PLC notification handle deleted"); rec != nil {
		t.Errorf("orphan reaper deleted a handle created by this session: %q", rec.Message)
	}
	if rec := logs.findByMessage("received notification for unknown handle"); rec != nil {
		t.Errorf("unknown-handle warning during batch subscribe (early sample not buffered): %q", rec.Message)
	}
}
