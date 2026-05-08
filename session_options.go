package ads

import (
	"log/slog"
	"time"
)

// SessionOption configures optional parameters for NewSession.
type SessionOption func(*Session)

// WithLogger sets the logger for the Session and the underlying Client.
// If not provided, slog.Default() is used.
func WithLogger(logger *slog.Logger) SessionOption {
	return func(s *Session) {
		if logger != nil {
			s.logger = logger
		}
	}
}

// WithHostIP sets the IP address the PLC should use to reach this client.
// Required in Docker/VPN/NAT scenarios where the local TCP socket address
// differs from the externally routable IP. When set, AddRoute uses this IP
// as the callback address instead of deriving it from the AMS NetID.
func WithHostIP(ip string) SessionOption {
	return func(s *Session) {
		s.callbackIP = ip
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
func WithRoute(routeName, username, password string) SessionOption {
	return func(s *Session) {
		s.route.name = routeName
		s.route.username = username
		s.route.password = secret(password)
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
func WithBackoff(cfg BackoffConfig) SessionOption {
	return func(s *Session) {
		s.lifecycle.backoffConfig = cfg
	}
}

// WithMaxReconnectAttempts limits total TCP reconnection attempts before giving up.
// Default is 0 (infinite retries). When the limit is reached, the reconnect
// goroutine returns an error and the connection stays in disconnected state.
func WithMaxReconnectAttempts(n int) SessionOption {
	return func(s *Session) {
		s.lifecycle.maxReconnectAttempts = n
	}
}

// WithRequestTimeout overrides the per-request timeout for ADS commands and
// initial-dial timeout. Defaults to the value passed as requestTimeout in
// NewSession (or 5s if that was zero). Useful for slow PLCs or networks
// where a single command may legitimately take longer than the default.
//
// Note: this option is also used as the net.DialTimeout for initial Connect
// and reconnect dial. A single value covers both ADS request and TCP dial
// semantics — split if you need different deadlines.
func WithRequestTimeout(d time.Duration) SessionOption {
	return func(s *Session) {
		if d > 0 {
			s.requestTimeout = d
		}
	}
}

// WithForceRouteRegistration disables route probing and always registers the route
// with credentials on every Connect and Reconnect. Use this in environments where
// routes are not persistent or must be refreshed on each connection.
func WithForceRouteRegistration() SessionOption {
	return func(s *Session) {
		s.route.forceRouteRegistration = true
	}
}

// WithStrictReconnect makes reconnection fail if any previously-resolved on-demand
// symbol is no longer available on the PLC (e.g., after an online change).
// By default, missing symbols are skipped gracefully during reconnect.
// maxAttempts controls how many reconnect attempts are allowed before giving up:
//   - 0 = fail immediately on first missing symbol
//   - N > 0 = retry up to N times, then return error (connection closes)
func WithStrictReconnect(maxAttempts int) SessionOption {
	return func(s *Session) {
		s.lifecycle.strictReconnect = true
		s.lifecycle.strictReconnectMaxAttempts = maxAttempts
	}
}

// WithAutoReconnect controls whether the connection automatically reconnects
// when the TCP connection drops. Default is true.
// When disabled, triggerReconnect sets the transport-down flag but does not launch
// a reconnect goroutine. Pending and subsequent RPCs return ErrDisconnected.
// The caller must call Reconnect() manually to re-establish the connection.
func WithAutoReconnect(enabled bool) SessionOption {
	return func(s *Session) {
		s.lifecycle.autoReconnect = enabled
	}
}

// WithOnDisconnect registers a callback invoked when a disconnect is detected.
// The callback runs in a separate goroutine and must not block.
func WithOnDisconnect(fn func()) SessionOption {
	return func(s *Session) {
		s.onDisconnect = fn
	}
}

// WithOnReconnect registers a callback invoked after a successful reconnect.
// The callback runs in a separate goroutine and must not block.
func WithOnReconnect(fn func()) SessionOption {
	return func(s *Session) {
		s.onReconnect = fn
	}
}
