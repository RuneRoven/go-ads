//go:build integration

package ads

import (
	"context"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"
)

// connDefaults holds default values for integration test connections.
type connDefaults struct {
	ip        string // default PLC IP
	targetAMS string // default AMS NetID
	routeName string // route name registered on PLC
}

// setupConnectionWithDefaults creates a PLC connection using env vars with provided fallbacks.
func setupConnectionWithDefaults(t *testing.T, d connDefaults) *Session {
	t.Helper()

	ip := getEnvOrDefault("ADS_PLC_IP", d.ip)
	targetAMS := getEnvOrDefault("ADS_TARGET_AMS", d.targetAMS)
	targetPortStr := getEnvOrDefault("ADS_TARGET_PORT", "851")
	targetPort, err := strconv.Atoi(targetPortStr)
	if err != nil {
		t.Fatalf("invalid ADS_TARGET_PORT %q: %v", targetPortStr, err)
	}
	localAMS := getEnvOrDefault("ADS_LOCAL_AMS", "auto")

	var opts []SessionOption
	hostIP := os.Getenv("ADS_HOST_IP")
	if hostIP != "" {
		opts = append(opts, WithHostIP(hostIP))
	}
	routeUser := os.Getenv("ADS_ROUTE_USER")
	routePass := os.Getenv("ADS_ROUTE_PASS")
	// ADS_SKIP_ROUTE_REGISTER=true → don't pass WithRoute → ensureRoute no-op.
	// Requires route to be pre-registered on PLC. Used for multi-session tests
	// where AddRoute UDP would terminate sibling TCP connections.
	skipRouteReg := strings.EqualFold(os.Getenv("ADS_SKIP_ROUTE_REGISTER"), "true")
	// Resolve effective route name:
	//   1. ADS_ROUTE_NAME explicit override → use as-is
	//   2. ADS_HOST_IP set → derive "go-ads-<ip>" so each source IP creates a
	//      distinct PLC route entry. Avoids the duplicate-name collision
	//      observed when the same host's IP changes (wifi↔ethernet, DHCP
	//      lease) and a stale entry blocks new registrations.
	//   3. Fall back to the connDefaults.routeName supplied by the caller.
	routeName := d.routeName
	if envName := os.Getenv("ADS_ROUTE_NAME"); envName != "" {
		routeName = envName
	} else if hostIP != "" {
		routeName = "go-ads-" + strings.ReplaceAll(hostIP, ".", "-")
	}
	if !skipRouteReg && routeUser != "" && routePass != "" {
		opts = append(opts, WithRoute(routeName, routeUser, routePass))
	}

	target, err := NewAMSAddress(targetAMS, uint16(targetPort))
	if err != nil {
		t.Fatalf("invalid target AMS: %v", err)
	}
	opts = append(opts, WithRequestTimeout(5*time.Second), WithLocalAMS(AMSAddress{Port: 10500}))
	if localAMS != "auto" && localAMS != "" {
		local, err := NewAMSAddress(localAMS, 10500)
		if err != nil {
			t.Fatalf("invalid local AMS: %v", err)
		}
		opts = append(opts, WithLocalAMS(local))
	}
	conn, err := NewSession(context.Background(), AMSEndpoint{IP: ip, Port: 48898, AMS: target}, opts...)
	if err != nil {
		t.Fatalf("NewConnection failed: %v", err)
	}

	err = conn.Connect(context.Background())
	if err != nil {
		t.Fatalf("Connect failed: %v", err)
	}

	t.Cleanup(func() {
		conn.Close()
	})

	return conn
}
