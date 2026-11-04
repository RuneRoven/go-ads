package ads

import (
	"context"
	"errors"
	"testing"
	"time"
)

// Every call that needs the PLC returns ErrDisconnected on a session that was
// never connected, instead of dereferencing the missing client.
func TestSession_CallsBeforeConnectReturnErrDisconnected(t *testing.T) {
	ctx := context.Background()
	cfg := SlowDiscoveryConfig{}
	calls := map[string]func(s *Session) error{
		"AddSymbolNotifications": func(s *Session) error {
			// A batch reports per entry; every entry must carry the refusal.
			res, err := s.AddSymbolNotifications(ctx, []NotificationConfig{{SymbolName: "MAIN.a"}, {SymbolName: "MAIN.b"}}, make(chan *Update, 1))
			if err != nil {
				return err
			}
			for _, r := range res {
				if !errors.Is(r.Skipped, ErrDisconnected) {
					return r.Skipped
				}
			}
			return res[0].Skipped
		},
		"AddSymbolNotification": func(s *Session) error {
			_, err := s.AddSymbolNotification(ctx, "MAIN.a", 0, time.Second, TransModeServerCycle, make(chan *Update, 1))
			return err
		},
		"ReadFromSymbol":      func(s *Session) error { _, err := s.ReadFromSymbol(ctx, "MAIN.a"); return err },
		"WriteToSymbol":       func(s *Session) error { return s.WriteToSymbol(ctx, "MAIN.a", "1") },
		"ReadMultipleSymbols": func(s *Session) error { _, err := s.ReadMultipleSymbols(ctx, []string{"MAIN.a", "MAIN.b"}); return err },
		"WriteMultipleSymbols": func(s *Session) error {
			_, err := s.WriteMultipleSymbols(ctx, map[string]string{"MAIN.a": "1"})
			return err
		},
		"GetSymbol":                   func(s *Session) error { _, err := s.GetSymbol(ctx, "MAIN.a"); return err },
		"LoadSymbols":                 func(s *Session) error { return s.LoadSymbols(ctx) },
		"LoadSymbolsSlow":             func(s *Session) error { return s.LoadSymbolsSlow(ctx, cfg) },
		"LoadSymbolList":              func(s *Session) error { return s.LoadSymbolList(ctx, cfg) },
		"LoadDataTypes":               func(s *Session) error { return s.LoadDataTypes(ctx, cfg) },
		"RefreshSymbols":              func(s *Session) error { return s.RefreshSymbols(ctx) },
		"CheckSymbolVersion":          func(s *Session) error { _, err := s.CheckSymbolVersion(ctx); return err },
		"RuntimeState":                func(s *Session) error { _, err := s.RuntimeState(ctx); return err },
		"DeleteDeviceNotification":    func(s *Session) error { return s.DeleteDeviceNotification(ctx, 1) },
		"SumDeleteDeviceNotification": func(s *Session) error { _, err := s.SumDeleteDeviceNotification(ctx, []uint32{1}); return err },
	}
	for name, call := range calls {
		t.Run(name, func(t *testing.T) {
			target, err := NewAMSAddress("127.0.0.1.1.1", 851)
			if err != nil {
				t.Fatal(err)
			}
			s, err := NewSession(ctx, AMSEndpoint{IP: "127.0.0.1", Port: 1, AMS: target}, WithAutoReconnect(false))
			if err != nil {
				t.Fatalf("NewSession: %v", err)
			}
			defer s.Close()
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("panicked: %v", r)
				}
			}()
			if err := call(s); !errors.Is(err, ErrDisconnected) {
				t.Errorf("err = %v, want ErrDisconnected", err)
			}
		})
	}
}
