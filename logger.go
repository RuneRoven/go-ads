package ads

import (
	"encoding/hex"
	"log/slog"
	"sync/atomic"
	"time"
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

// WithRoute configures automatic AMS route registration during Connect().
// The route is registered via UDP (port 48899) after the TCP connection is established
// and the source AMS NetID is derived, but before any ADS commands are sent.
// By default, Connect and Reconnect probe the PLC first (via GetSymbolVersion) to check
// if the route already exists, and only register with credentials if the probe fails.
// Use WithForceRouteRegistration to always register without probing.
//
// Security: Beckhoff's route registration protocol transmits credentials in cleartext
// over UDP. This is a protocol-level limitation — there is no encrypted alternative.
// Ensure route registration only occurs on trusted networks.
func WithRoute(routeName, username, password string) ConnectionOption {
	return func(c *Connection) {
		c.routeName = routeName
		c.routeUsername = username
		c.routePassword = secret(password)
	}
}

// BackoffConfig controls reconnect timing behavior.
// Reconnection uses stepped intervals: fast retries first (for network blips),
// then progressively slower intervals to avoid overwhelming the PLC.
// Backoff resets on each successful reconnect.
type BackoffConfig struct {
	InitialInterval time.Duration // delay for first N attempts (default: 1s)
	InitialAttempts int           // how many attempts at initial interval (default: 3)
	MidInterval     time.Duration // delay for mid-tier attempts (default: 5s)
	MidAttempts     int           // how many attempts at mid interval (default: 3)
	SlowInterval    time.Duration // delay for slow-tier attempts (default: 15s)
	SlowAttempts    int           // how many attempts at slow interval (default: 4)
	MaxInterval     time.Duration // cap after all tiers exhausted (default: 30s)
}

// DefaultBackoffConfig returns the default reconnect backoff configuration.
func DefaultBackoffConfig() BackoffConfig {
	return BackoffConfig{
		InitialInterval: 1 * time.Second,
		InitialAttempts: 3,
		MidInterval:     5 * time.Second,
		MidAttempts:     3,
		SlowInterval:    15 * time.Second,
		SlowAttempts:    4,
		MaxInterval:     30 * time.Second,
	}
}

// WithBackoff sets the reconnect backoff configuration.
// If not provided, DefaultBackoffConfig() is used.
func WithBackoff(cfg BackoffConfig) ConnectionOption {
	return func(c *Connection) {
		c.backoffConfig = cfg
	}
}

// WithMaxReconnectAttempts limits total TCP reconnection attempts before giving up.
// Default is 0 (infinite retries). When the limit is reached, the reconnect
// goroutine returns an error and the connection stays in disconnected state.
func WithMaxReconnectAttempts(n int) ConnectionOption {
	return func(c *Connection) {
		c.maxReconnectAttempts = n
	}
}

// WithForceRouteRegistration disables route probing and always registers the route
// with credentials on every Connect and Reconnect. Use this in environments where
// routes are not persistent or must be refreshed on each connection.
func WithForceRouteRegistration() ConnectionOption {
	return func(c *Connection) {
		c.forceRouteRegistration = true
	}
}

// WithStrictReconnect makes reconnection fail if any previously-resolved on-demand
// symbol is no longer available on the PLC (e.g., after an online change).
// By default, missing symbols are skipped gracefully during reconnect.
// maxAttempts controls how many reconnect attempts are allowed before giving up:
//   - 0 = fail immediately on first missing symbol
//   - N > 0 = retry up to N times, then return error (connection closes)
func WithStrictReconnect(maxAttempts int) ConnectionOption {
	return func(c *Connection) {
		c.strictReconnect = true
		c.strictReconnectMaxAttempts = maxAttempts
	}
}

// WithAutoReconnect controls whether the connection automatically reconnects
// when the TCP connection drops. Default is true.
// When disabled, triggerReconnect sets the disconnected flag but does not launch
// a reconnect goroutine. sendRequest returns ErrDisconnected immediately.
// The caller must call Reconnect() manually to re-establish the connection.
func WithAutoReconnect(enabled bool) ConnectionOption {
	return func(c *Connection) {
		c.autoReconnect = enabled
	}
}

// WithOnDisconnect registers a callback invoked when a disconnect is detected.
// The callback runs in a separate goroutine and must not block.
func WithOnDisconnect(fn func()) ConnectionOption {
	return func(c *Connection) {
		c.onDisconnect = fn
	}
}

// WithOnReconnect registers a callback invoked after a successful reconnect.
// The callback runs in a separate goroutine and must not block.
func WithOnReconnect(fn func()) ConnectionOption {
	return func(c *Connection) {
		c.onReconnect = fn
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
