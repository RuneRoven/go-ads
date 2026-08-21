package ads

import (
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"
)

// newTargetCheckSession builds the minimum Session applyTargetCheck touches,
// with a capturing logger.
func newTargetCheckSession(t *testing.T, netID string, check TargetCheck) (*Session, *testLogHandler) {
	t.Helper()
	target, err := NewAMSAddress(netID, 851)
	if err != nil {
		t.Fatalf("target %q: %v", netID, err)
	}
	logs := &testLogHandler{}
	return &Session{
		ip:          "192.168.3.118",
		target:      target,
		targetCheck: check,
		logger:      slog.New(logs),
	}, logs
}

func identityOf(t *testing.T, netID string) RemoteIdentity {
	t.Helper()
	ams, err := NewAMSAddress(netID, 10000)
	if err != nil {
		t.Fatalf("identity %q: %v", netID, err)
	}
	return RemoteIdentity{AMS: ams, HostName: "CX-4285CB", Major: 3, Minor: 1, Build: 4024}
}

// TestApplyTargetCheck_Match: the device agrees, so nothing is reported to the
// operator beyond Debug.
func TestApplyTargetCheck_Match(t *testing.T) {
	sess, logs := newTargetCheckSession(t, "5.66.133.203.1.1", TargetCheckWarn)
	if err := sess.applyTargetCheck(identityOf(t, "5.66.133.203.1.1")); err != nil {
		t.Fatalf("applyTargetCheck on a matching NetID: %v", err)
	}
	if rec := logs.findByMessage("differs from"); rec != nil {
		t.Errorf("unexpected mismatch log on a match: %q", rec.Message)
	}
	if rec := logs.findByMessage("confirmed by device"); rec == nil {
		t.Error("no confirmation logged")
	} else if rec.Level != slog.LevelDebug {
		t.Errorf("confirmation logged at %v, want Debug (a match is not news)", rec.Level)
	}
}

// TestApplyTargetCheck_MismatchWarns: the default must not refuse, because the
// same signature is produced by a legitimate routed setup.
func TestApplyTargetCheck_MismatchWarns(t *testing.T) {
	sess, logs := newTargetCheckSession(t, "5.1.2.3.1.1", TargetCheckWarn)
	if err := sess.applyTargetCheck(identityOf(t, "5.66.133.203.1.1")); err != nil {
		t.Fatalf("TargetCheckWarn returned an error: %v", err)
	}
	rec := logs.findByMessage("differs from")
	if rec == nil {
		t.Fatal("mismatch not logged")
	}
	if rec.Level != slog.LevelWarn {
		t.Errorf("mismatch logged at %v, want Warn", rec.Level)
	}
	// Downstream log-based health checks fail on WARN lines containing these
	// substrings, and a warning that trips them would be worse than useless.
	for _, banned := range []string{"failed to", "unable to", "connection lost"} {
		if strings.Contains(rec.Message, banned) {
			t.Errorf("warn message contains %q, which trips downstream log checks: %q", banned, rec.Message)
		}
	}
}

// TestApplyTargetCheck_MismatchErrors: strict mode refuses, and the error has to
// carry both NetIDs and the responder's identity — the whole point is that the
// reader can tell which end is wrong without further digging.
func TestApplyTargetCheck_MismatchErrors(t *testing.T) {
	sess, _ := newTargetCheckSession(t, "5.1.2.3.1.1", TargetCheckError)
	err := sess.applyTargetCheck(identityOf(t, "5.66.133.203.1.1"))
	if err == nil {
		t.Fatal("TargetCheckError accepted a mismatch")
	}
	for _, want := range []string{"5.1.2.3.1.1", "5.66.133.203.1.1", "192.168.3.118", "CX-4285CB", "3.1.4024"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error missing %q: %v", want, err)
		}
	}
}

// TestApplyTargetCheck_ErrorModeAcceptsMatch guards against strict mode being
// so strict it rejects a correct address.
func TestApplyTargetCheck_ErrorModeAcceptsMatch(t *testing.T) {
	sess, _ := newTargetCheckSession(t, "5.66.133.203.1.1", TargetCheckError)
	if err := sess.applyTargetCheck(identityOf(t, "5.66.133.203.1.1")); err != nil {
		t.Errorf("TargetCheckError rejected a matching NetID: %v", err)
	}
}

// TestWithTargetCheck: the option overrides the default, and the zero value is
// ignored so it cannot silently disable the check.
func TestWithTargetCheck(t *testing.T) {
	tests := []struct {
		name string
		set  TargetCheck
		want TargetCheck
	}{
		{name: "error mode", set: TargetCheckError, want: TargetCheckError},
		{name: "off", set: TargetCheckOff, want: TargetCheckOff},
		{name: "zero value keeps the default", set: 0, want: TargetCheckWarn},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sess := &Session{targetCheck: TargetCheckWarn}
			WithTargetCheck(tt.set)(sess)
			if sess.targetCheck != tt.want {
				t.Errorf("targetCheck = %d, want %d", sess.targetCheck, tt.want)
			}
		})
	}
}

// TestNewSession_LocalModeSkipsDiscovery: local mode targets the in-process
// runtime and Connect overwrites NetID and IP with loopback regardless, so
// probing the caller-supplied address at construction is pointless — and its
// failure must not sink the session. The address here is chosen to be
// unroutable, so a probe would have to time out.
func TestNewSession_LocalModeSkipsDiscovery(t *testing.T) {
	start := time.Now()
	sess, err := NewSession(context.Background(),
		AMSEndpoint{IP: "192.0.2.1"}, // RFC 5737 TEST-NET-1: guaranteed unroutable
		WithLocalMode())
	if err != nil {
		t.Fatalf("NewSession in local mode with no target AMS: %v", err)
	}
	t.Cleanup(func() { sess.Close() })

	// identifyTimeout is 3s; anything near it means a probe was attempted.
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("NewSession took %v — local mode probed the network", elapsed)
	}
	if sess.target.NetID != [6]byte{} {
		t.Errorf("target NetID = %s, want zero (Connect assigns the loopback NetID)", sess.target.NetIDString())
	}
}
