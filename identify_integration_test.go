//go:build integration

package ads

import (
	"context"
	"os"
	"testing"
)

// TestIntegrationIdentifyRemote checks discovery against the configured target:
// whatever the device reports must match the NetID the env file was written
// with. That makes this test double as a check that the env file has not gone
// stale, which is a failure mode that otherwise looks like a dead PLC.
func TestIntegrationIdentifyRemote(t *testing.T) {
	host := getEnvOrDefault("ADS_PLC_IP", "192.168.3.224")
	want := os.Getenv("ADS_TARGET_AMS")

	id, err := IdentifyRemote(context.Background(), host)
	if err != nil {
		t.Fatalf("IdentifyRemote(%s): %v", host, err)
	}
	t.Logf("%s: netID=%s host=%q twinCAT=%s runtimePort=%d",
		host, id.AMS.NetIDString(), id.HostName, id.Version(), id.RuntimePort())

	if want != "" && id.AMS.NetIDString() != want {
		t.Errorf("discovered NetID = %s, want %s (ADS_TARGET_AMS) — the env file or the device changed",
			id.AMS.NetIDString(), want)
	}
	if id.AMS.Port != 10000 {
		t.Errorf("reported port = %d, want 10000 (the router's own port)", id.AMS.Port)
	}
	if id.HostName == "" {
		t.Error("HostName empty; every tested TwinCAT reports one")
	}
	if id.Major == 0 {
		t.Error("TwinCAT major version not reported")
	}
	// The port convention has to agree with how this PLC is actually addressed.
	if envPort := os.Getenv("ADS_TARGET_PORT"); envPort == "801" && id.RuntimePort() != 801 {
		t.Errorf("RuntimePort() = %d on a TwinCAT %s device configured for 801", id.RuntimePort(), id.Version())
	}
}

// TestIntegrationSessionDiscoversTarget leaves the target AMS address out
// entirely and requires NewSession to resolve it and then actually work.
func TestIntegrationSessionDiscoversTarget(t *testing.T) {
	host := getEnvOrDefault("ADS_PLC_IP", "192.168.3.224")
	wantNetID := os.Getenv("ADS_TARGET_AMS")

	var opts []SessionOption
	if hostIP := os.Getenv("ADS_HOST_IP"); hostIP != "" {
		opts = append(opts, WithHostIP(hostIP))
	}
	if user, pass := os.Getenv("ADS_ROUTE_USER"), os.Getenv("ADS_ROUTE_PASS"); user != "" && pass != "" {
		routeName := "go-ads-discover"
		opts = append(opts, WithRoute(routeName, user, pass))
	}
	if localAMS := os.Getenv("ADS_LOCAL_AMS"); localAMS != "" {
		local, err := NewAMSAddress(localAMS, 10500)
		if err != nil {
			t.Fatalf("ADS_LOCAL_AMS: %v", err)
		}
		opts = append(opts, WithLocalAMS(local))
	}

	// No AMS field at all: NetID and port both come from the device.
	sess, err := NewSession(context.Background(), AMSEndpoint{IP: host}, opts...)
	if err != nil {
		t.Fatalf("NewSession without a target AMS address: %v", err)
	}
	t.Cleanup(func() { sess.Close() })

	if wantNetID != "" && sess.target.NetIDString() != wantNetID {
		t.Errorf("resolved NetID = %s, want %s", sess.target.NetIDString(), wantNetID)
	}
	if envPort := os.Getenv("ADS_TARGET_PORT"); envPort != "" {
		t.Logf("resolved port %d, env says %s", sess.target.Port, envPort)
	}

	// Resolution is only worth anything if the session then works.
	if err := sess.Connect(context.Background()); err != nil {
		t.Fatalf("Connect with a discovered target: %v", err)
	}
	version, err := sess.client.Load().GetSymbolVersion(context.Background())
	if err != nil {
		t.Fatalf("GetSymbolVersion over a discovered target: %v", err)
	}
	t.Logf("connected via discovered target %s: symbol version %d", sess.target.String(), version)
}
