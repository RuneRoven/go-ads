package ads

import (
	"encoding/hex"
	"log/slog"
	"sync/atomic"
)

// LevelTrace is a custom slog level for trace-level logging,
// below slog.LevelDebug (-4). Matches zerolog's Trace level semantics.
const LevelTrace = slog.Level(-8)

// hexAttr returns a slog.Attr that formats a byte slice as a hex string.
// Equivalent to zerolog's .Hex("key", data) method.
func hexAttr(key string, data []byte) slog.Attr {
	return slog.String(key, hex.EncodeToString(data))
}

// ConnectionOption configures optional parameters for NewConnection.
type ConnectionOption func(*Connection)

// WithLogger sets the logger for the connection.
// If not provided, slog.Default() is used.
func WithLogger(logger *slog.Logger) ConnectionOption {
	return func(c *Connection) {
		if logger != nil {
			c.logger = logger
		}
	}
}

// WithHostIP sets the IP address the PLC should use to reach this client.
// Required in Docker/VPN/NAT scenarios where the local TCP socket address
// differs from the externally routable IP. When set, AddRoute uses this IP
// as the callback address instead of deriving it from the AMS NetID.
func WithHostIP(ip string) ConnectionOption {
	return func(c *Connection) {
		c.callbackIP = ip
	}
}

// defaultLoggerPtr is used by package-level functions (e.g., AddRemoteRoute)
// that are not associated with a Connection instance.
var defaultLoggerPtr atomic.Pointer[slog.Logger]

func init() {
	d := slog.Default()
	defaultLoggerPtr.Store(d)
}

func getDefaultLogger() *slog.Logger {
	return defaultLoggerPtr.Load()
}

// SetDefaultLogger sets the package-level logger used by standalone functions
// like AddRemoteRoute. Call this before creating any connections if you want
// package-level functions to use a custom logger.
func SetDefaultLogger(logger *slog.Logger) {
	if logger != nil {
		defaultLoggerPtr.Store(logger)
	}
}
