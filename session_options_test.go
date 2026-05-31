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
	called := make(chan Reason, 1)
	WithOnSymbolVersionChanged(func(reason Reason) { called <- reason })(s)
	if s.versionCallback == nil {
		t.Fatal("callback not stored")
	}
	s.versionCallback(ReasonSymbolVersionInvalid)
	got := <-called
	if got != ReasonSymbolVersionInvalid {
		t.Errorf("callback received %q, want %q", got, ReasonSymbolVersionInvalid)
	}
}

func TestWithSkipRouteRegistration(t *testing.T) {
	s := &Session{route: &routeManager{}}

	// Without the option: shouldSkip is true only because name is empty.
	if !s.route.shouldSkip() {
		t.Fatal("zero-value routeManager should skip (empty name)")
	}

	// With WithRoute set, shouldSkip becomes false.
	WithRoute("name", "user", "pass")(s)
	if s.route.shouldSkip() {
		t.Errorf("after WithRoute, shouldSkip = true, want false")
	}

	// WithSkipRouteRegistration overrides — still skip even with WithRoute.
	WithSkipRouteRegistration()(s)
	if !s.route.skipRegistration {
		t.Errorf("skipRegistration = false, want true")
	}
	if !s.route.shouldSkip() {
		t.Errorf("shouldSkip = false after WithSkipRouteRegistration, want true")
	}
	if s.route.name != "name" {
		t.Errorf("WithSkipRouteRegistration should not clear name, got %q", s.route.name)
	}
}

func TestRouteManager_ShouldSkip(t *testing.T) {
	tests := []struct {
		name             string
		routeName        string
		skipRegistration bool
		want             bool
	}{
		{"empty name → skip", "", false, true},
		{"name set, no skip → register", "myroute", false, false},
		{"name set + explicit skip → skip", "myroute", true, true},
		{"empty name + explicit skip → skip", "", true, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := &routeManager{name: tc.routeName, skipRegistration: tc.skipRegistration}
			if got := r.shouldSkip(); got != tc.want {
				t.Errorf("shouldSkip() = %v, want %v", got, tc.want)
			}
		})
	}
}
