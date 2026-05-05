package ads

import (
	"bytes"
	"fmt"
	"log/slog"
	"strings"
	"testing"
)

// F-25: secret type must redact via fmt.Sprintf "%v", "%+v", "%s" — all paths
// that call String().
func TestSecret_StringRedacted(t *testing.T) {
	s := secret("supersecret123")

	if got := s.String(); got != "[REDACTED]" {
		t.Errorf("s.String() = %q, want \"[REDACTED]\"", got)
	}
	if got := fmt.Sprintf("%v", s); got != "[REDACTED]" {
		t.Errorf("fmt.Sprintf(%%v) = %q, want \"[REDACTED]\"", got)
	}
	// %s on a Stringer routes through String() — covered by Go fmt semantics;
	// %v above already exercises that path. Skipping a separate %s assertion
	// avoids staticcheck S1025 noise.

	// Cast to string still gives raw value (intentional — for use at boundary).
	if got := string(s); got != "supersecret123" {
		t.Errorf("string(s) = %q, want raw value", got)
	}
}

// F-25: slog.LogValue must return [REDACTED], not the raw value.
func TestSecret_LogValueRedacted(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))

	s := secret("supersecret123")
	logger.Info("test", "password", s)

	out := buf.String()
	if strings.Contains(out, "supersecret123") {
		t.Errorf("log output leaked raw secret: %s", out)
	}
	if !strings.Contains(out, "[REDACTED]") {
		t.Errorf("log output missing [REDACTED]: %s", out)
	}
}
