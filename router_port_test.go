package ads

import (
	"context"
	"encoding/binary"
	"net"
	"strings"
	"sync"
	"testing"
	"time"
)

// router_port_test.go — AMSEndpoint.RouterPort.
//
// A PLC behind NAT is reached on forwarded ports, and NAT maps one external
// port per internal port: the forwarded UDP port is a different number than the
// forwarded TCP port and cannot be derived from it. So the router port has to be
// independently settable, and the UDP calls (route registration, identify) have
// to use it rather than the protocol constant.

// TestNewSession_RouterPortDefaultsToProtocolPort: leaving it unset keeps the
// TwinCAT default, so nothing changes for a directly-reachable PLC.
func TestNewSession_RouterPortDefaultsToProtocolPort(t *testing.T) {
	sess, err := NewSession(context.Background(), AMSEndpoint{
		IP:  "127.0.0.1",
		AMS: AMSAddress{NetID: [6]byte{5, 1, 2, 3, 1, 1}, Port: 851},
	}, WithTargetCheck(TargetCheckOff))
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	t.Cleanup(func() { sess.Close() })
	if got := sess.effectiveRouterPort(); got != routePort {
		t.Errorf("router port = %d, want the protocol default %d", got, routePort)
	}
	if sess.port != 48898 {
		t.Errorf("TCP port = %d, want the protocol default 48898", sess.port)
	}
}

// TestNewSession_RouterPortIndependentOfTCPPort is the NAT shape: two unrelated
// forwarded numbers, neither derived from the other.
func TestNewSession_RouterPortIndependentOfTCPPort(t *testing.T) {
	sess, err := NewSession(context.Background(), AMSEndpoint{
		IP:         "127.0.0.1",
		Port:       5534, // external TCP -> 48898 on the PLC
		RouterPort: 6499, // external UDP -> 48899 on the PLC
		AMS:        AMSAddress{NetID: [6]byte{5, 1, 2, 3, 1, 1}, Port: 851},
	}, WithTargetCheck(TargetCheckOff))
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	t.Cleanup(func() { sess.Close() })
	if sess.port != 5534 {
		t.Errorf("TCP port = %d, want 5534", sess.port)
	}
	if got := sess.effectiveRouterPort(); got != 6499 {
		t.Errorf("router port = %d, want 6499", got)
	}
}

// TestSplitHostRouterPort: the standalone IdentifyRemote / AddRemoteRoute
// helpers take no port argument, so a NAT-forwarded router is only reachable
// through them if they read the port off the host string.
//
// A port that is present but unusable has to be an error. Folding it back into
// the hostname — the first shape of this helper — meant "10.0.0.5:99999" was
// handed to LookupIPAddr as a hostname and surfaced as "no such host", naming an
// address the user never typed and never mentioning the port. Worse for a NAT
// feature: a mistyped forwarded port silently fell back to 48899, which on the
// NAT host may well answer for a different device.
func TestSplitHostRouterPort(t *testing.T) {
	tests := []struct {
		name     string
		in       string
		wantHost string
		wantPort int
		wantErr  bool
	}{
		{name: "bare ip", in: "192.168.3.70", wantHost: "192.168.3.70", wantPort: routePort},
		{name: "bare hostname", in: "plc.local", wantHost: "plc.local", wantPort: routePort},
		{name: "ip with port", in: "192.168.3.70:6499", wantHost: "192.168.3.70", wantPort: 6499},
		{name: "hostname with port", in: "plc.local:6499", wantHost: "plc.local", wantPort: 6499},
		// Parsed, not supported: both callers dial udp4 and reject a non-IPv4
		// address later with a message that says so. This pins the parse only.
		{name: "bracketed ipv6 with port parses", in: "[fd00::1]:6499", wantHost: "fd00::1", wantPort: 6499},
		{name: "bare ipv6 is not a host:port", in: "fd00::1", wantHost: "fd00::1", wantPort: routePort},
		{name: "port zero", in: "192.168.3.70:0", wantErr: true},
		{name: "port out of range", in: "192.168.3.70:99999", wantErr: true},
		{name: "port not a number", in: "192.168.3.70:notaport", wantErr: true},
		{name: "empty port", in: "192.168.3.70:", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			host, port, err := splitHostRouterPort(tt.in)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("splitHostRouterPort(%q) = (%q, %d, nil), want an error naming the bad port",
						tt.in, host, port)
				}
				if !strings.Contains(err.Error(), tt.in) {
					t.Errorf("error %q does not name the input %q", err, tt.in)
				}
				return
			}
			if err != nil {
				t.Fatalf("splitHostRouterPort(%q): %v", tt.in, err)
			}
			if host != tt.wantHost || port != tt.wantPort {
				t.Errorf("splitHostRouterPort(%q) = (%q, %d), want (%q, %d)",
					tt.in, host, port, tt.wantHost, tt.wantPort)
			}
		})
	}
}

// TestSessionUsesRouterPortForIdentify proves the plumbing end to end for the
// UDP half: a responder on an arbitrary port is found only if the session
// actually probes RouterPort rather than the constant.
func TestSessionUsesRouterPortForIdentify(t *testing.T) {
	// Keep re-binding until the stub owns a port that is NOT the protocol
	// default: on the default port the test would pass even if the session
	// ignored RouterPort entirely. Skipping instead of retrying would silently
	// turn the only end-to-end proof of this plumbing into a no-op.
	var pc *net.UDPConn
	var addr *net.UDPAddr
	for attempt := 0; ; attempt++ {
		if attempt == 10 {
			t.Fatal("could not bind a UDP port other than the protocol default after 10 tries")
		}
		conn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
		if err != nil {
			t.Fatalf("listen udp: %v", err)
		}
		local, ok := conn.LocalAddr().(*net.UDPAddr)
		if !ok {
			_ = conn.Close()
			t.Fatalf("unexpected addr type: %T", conn.LocalAddr())
		}
		if local.Port != routePort {
			pc, addr = conn, local
			break
		}
		_ = conn.Close()
	}

	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		buf := make([]byte, 2048)
		for {
			select {
			case <-done:
				return
			default:
			}
			_ = pc.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
			_, from, err := pc.ReadFromUDP(buf)
			if err != nil {
				continue
			}
			invokeID := binary.LittleEndian.Uint32(buf[4:8])
			resp := buildIdentifyResponse(invokeID, [6]byte{5, 9, 8, 7, 1, 1}, 10000, [][2]any{
				{int(tagComputerName), append([]byte("NAT-PLC"), 0)},
				{int(tagSystemVersion), []byte{3, 1, 0xB8, 0x0F}},
			})
			_, _ = pc.WriteToUDP(resp, from)
		}
	}()
	defer func() { close(done); _ = pc.Close(); wg.Wait() }()

	// No target AMS at all, so NewSession must discover it — over RouterPort.
	sess, err := NewSession(context.Background(), AMSEndpoint{
		IP:         addr.IP.String(),
		Port:       5534, // nothing listens here; discovery is UDP-only
		RouterPort: addr.Port,
	})
	if err != nil {
		t.Fatalf("NewSession with discovery over a forwarded router port: %v", err)
	}
	t.Cleanup(func() { sess.Close() })

	if got := sess.target.NetIDString(); got != "5.9.8.7.1.1" {
		t.Errorf("discovered NetID = %s, want 5.9.8.7.1.1", got)
	}
	if sess.target.Port != 851 {
		t.Errorf("discovered runtime port = %d, want 851 for the reported TwinCAT 3", sess.target.Port)
	}
}
