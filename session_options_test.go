package ads

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"
	"time"
)

// Validates: R-SES-011.
func TestWithSymbolVersionStrategy(t *testing.T) {
	s := &Session{}
	WithSymbolVersionStrategy(SymbolVersionClose)(s)
	if s.versionStrategy != SymbolVersionClose {
		t.Errorf("strategy = %v, want Close", s.versionStrategy)
	}
}

// Validates: R-SES-011 default-behavior.
func TestSymbolVersionStrategy_ZeroValueIsAutoReload(t *testing.T) {
	s := &Session{}
	if s.versionStrategy != SymbolVersionAutoReload {
		t.Errorf("zero-value strategy = %v, want AutoReload", s.versionStrategy)
	}
}

// Validates: R-CACHE-013.
func TestWithMaxSymbolVersionReloadAttempts(t *testing.T) {
	s := &Session{}
	WithMaxSymbolVersionReloadAttempts(5)(s)
	if s.maxReloadAttempts != 5 {
		t.Errorf("max = %d, want 5", s.maxReloadAttempts)
	}
}

// Validates: R-CACHE-013 — n<1 silently rejected.
func TestWithMaxSymbolVersionReloadAttempts_RejectsZeroAndNeg(t *testing.T) {
	s := &Session{maxReloadAttempts: 7}
	WithMaxSymbolVersionReloadAttempts(0)(s)
	if s.maxReloadAttempts != 7 {
		t.Errorf("n=0 should be rejected, got max = %d", s.maxReloadAttempts)
	}
	WithMaxSymbolVersionReloadAttempts(-1)(s)
	if s.maxReloadAttempts != 7 {
		t.Errorf("n=-1 should be rejected, got max = %d", s.maxReloadAttempts)
	}
}

// Validates: R-CACHE-013.
func TestWithSymbolVersionReloadWindow(t *testing.T) {
	s := &Session{}
	WithSymbolVersionReloadWindow(30 * time.Second)(s)
	if s.reloadWindow != 30*time.Second {
		t.Errorf("window = %v, want 30s", s.reloadWindow)
	}
}

// Validates: R-CACHE-013 — d≤0 silently rejected.
func TestWithSymbolVersionReloadWindow_RejectsZeroAndNeg(t *testing.T) {
	s := &Session{reloadWindow: 5 * time.Second}
	WithSymbolVersionReloadWindow(0)(s)
	if s.reloadWindow != 5*time.Second {
		t.Errorf("d=0 should be rejected, got window = %v", s.reloadWindow)
	}
	WithSymbolVersionReloadWindow(-time.Second)(s)
	if s.reloadWindow != 5*time.Second {
		t.Errorf("d<0 should be rejected, got window = %v", s.reloadWindow)
	}
}

// Validates: R-SES-011 — invalid strategy values fall back to AutoReload + log Warn.
func TestWithSymbolVersionStrategy_InvalidFallsBack(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))
	s := &Session{logger: logger}
	WithSymbolVersionStrategy(SymbolVersionStrategy(99))(s)
	if s.versionStrategy != SymbolVersionAutoReload {
		t.Errorf("strategy = %v, want fallback AutoReload", s.versionStrategy)
	}
	if !strings.Contains(buf.String(), "invalid SymbolVersionStrategy") {
		t.Errorf("expected Warn log; got %q", buf.String())
	}
}

// Validates: R-SES-011 — invalid strategy on bare Session{} (nil logger) is
// the unit-test-friendly path: fallback applies, no panic.
func TestWithSymbolVersionStrategy_InvalidNoLoggerNoPanic(t *testing.T) {
	s := &Session{}
	WithSymbolVersionStrategy(SymbolVersionStrategy(42))(s)
	if s.versionStrategy != SymbolVersionAutoReload {
		t.Errorf("strategy = %v, want fallback AutoReload", s.versionStrategy)
	}
}

// Validates: R-SES-011 callback registration.
func TestWithOnSymbolVersionChanged(t *testing.T) {
	s := &Session{}
	called := make(chan string, 1)
	WithOnSymbolVersionChanged(func(reason string) { called <- reason })(s)
	if s.versionCallback == nil {
		t.Fatal("callback not stored")
	}
	s.versionCallback("test-reason")
	got := <-called
	if got != "test-reason" {
		t.Errorf("callback received %q, want test-reason", got)
	}
}
