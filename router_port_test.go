package ads

import (
	"context"
	"encoding/binary"
	"net"
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

// TestSessionUsesRouterPortForIdentify proves the plumbing end to end for the
// UDP half: a responder on an arbitrary port is found only if the session
// actually probes RouterPort rather than the constant.
func TestSessionUsesRouterPortForIdentify(t *testing.T) {
	pc, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatalf("listen udp: %v", err)
	}
	addr, ok := pc.LocalAddr().(*net.UDPAddr)
	if !ok {
		_ = pc.Close()
		t.Fatalf("unexpected addr type: %T", pc.LocalAddr())
	}
	if addr.Port == routePort {
		t.Skip("stub happened to bind the protocol port; the test would prove nothing")
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
