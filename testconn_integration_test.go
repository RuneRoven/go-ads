//go:build integration

package ads

import (
	"os"
	"strconv"
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
	if routeUser != "" && routePass != "" {
		opts = append(opts, WithRoute(d.routeName, routeUser, routePass))
	}

	conn, err := NewSession(ip, 48898, targetAMS, targetPort, localAMS, 10500, 5*time.Second, opts...)
	if err != nil {
		t.Fatalf("NewConnection failed: %v", err)
	}

	err = conn.Connect(false)
	if err != nil {
		t.Fatalf("Connect failed: %v", err)
	}

	t.Cleanup(func() {
		conn.Close()
	})

	return conn
}
