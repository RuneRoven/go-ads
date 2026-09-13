package ads

import (
	"fmt"
	"log/slog"
	"net"
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

// WithLocalBindIP forces the outbound TCP source IP; unset lets the OS pick. Used
// on hosts with IP aliases so each Session looks like a separate host and gets its
// own TCP slot -- TwinCAT allows one per source IP regardless of NetID (Beckhoff
// #49/#72). The alias must exist before Connect. An invalid IP is rejected at
// option time with a Warn, leaving OS-default routing.
func WithLocalBindIP(ip string) SessionOption {
	return func(s *Session) {
		if ip == "" {
			s.localBindIP = nil
			return
		}
		parsed := net.ParseIP(ip)
		if parsed == nil {
			if s.logger != nil {
				s.logger.Warn("WithLocalBindIP: invalid IP, ignoring (using OS-default routing)",
					"ip", ip)
			}
			return
		}
		s.localBindIP = parsed
	}
}

// WithLocalAMS sets the local (source) AMSAddress in outgoing ADS headers. NetID
// defaults to the local TCP source IP, Port to a random dynamic-range value. The
// AMS port is a logical id in the header, not the TCP source or destination port.
//
// HAZARD with route registration: TC3 keys its route table by ADDRESS, and two
// entries for one address take the router out of service for every client until it
// is cleared by hand and the device restarted. Auto-derivation keeps NetID and
// address in correspondence; overriding the NetID breaks it. Safe: a NetID
// matching the address, or WithSkipRouteRegistration. Avoid: two sessions from one
// host under different NetIDs, or changing the NetID between runs.
func WithLocalAMS(local AMSAddress) SessionOption {
	return func(s *Session) {
		if local.NetID != [6]byte{} {
			s.source.NetID = local.NetID
		}
		if local.Port != 0 {
			s.source.Port = local.Port
		}
	}
}

// WithLocalMode targets the in-process TwinCAT runtime at 127.0.0.1, used
// when the application runs on the same machine as the PLC runtime. Sets
// the local-mode flag that Connect uses to short-circuit the route probe
// and force the loopback target NetID 127.0.0.1.1.1.
func WithLocalMode() SessionOption {
	return func(s *Session) {
		s.isLocal = true
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

// WithSkipRouteRegistration explicitly disables AMS route registration during
// Connect() and Reconnect(). Use when routes are managed externally:
//   - Pre-registered on the PLC via TC3 UI / TC2 properties or AdsTool
//   - Owned by a local AMS router daemon (AmsRouterDaemon) that the Session
//     connects to instead of the PLC directly
//
// Equivalent to omitting WithRoute, but explicit: callers may still invoke
// WithRoute for documentation/auditing yet override here without changing
// the rest of the option chain. Bypasses both probe and AddRoute so no UDP
// traffic to port 48899 is generated.
func WithSkipRouteRegistration() SessionOption {
	return func(s *Session) {
		s.route.skipRegistration = true
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
//
// MaxInterval also caps the cross-cycle flap cooldown (see flapResetWindow), which
// is what governs a device that resets the connection on a timer. The 30s default
// protects the PLC's socket table at the cost of stream continuity during such an
// episode; lower it if the samples matter more than the sockets.
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

// Validate reports configuration problems that would produce pathological
// reconnect behavior: zero-or-negative intervals (zero-delay retry storms
// that exhaust ephemeral ports), negative attempt counts (skipped tiers
// surfacing as silent fast-fail), or a MaxInterval below the previous tier
// (caps that defeat the slow-tier ramp).
func (c BackoffConfig) Validate() error {
	if c.InitialInterval <= 0 {
		return fmt.Errorf("BackoffConfig.InitialInterval must be > 0 (got %v); zero-delay retries exhaust ephemeral ports", c.InitialInterval)
	}
	if c.MidInterval <= 0 {
		return fmt.Errorf("BackoffConfig.MidInterval must be > 0 (got %v)", c.MidInterval)
	}
	if c.SlowInterval <= 0 {
		return fmt.Errorf("BackoffConfig.SlowInterval must be > 0 (got %v)", c.SlowInterval)
	}
	if c.MaxInterval <= 0 {
		return fmt.Errorf("BackoffConfig.MaxInterval must be > 0 (got %v)", c.MaxInterval)
	}
	if c.InitialAttempts < 0 {
		return fmt.Errorf("BackoffConfig.InitialAttempts must be >= 0 (got %d); negative silently skips the tier", c.InitialAttempts)
	}
	if c.MidAttempts < 0 {
		return fmt.Errorf("BackoffConfig.MidAttempts must be >= 0 (got %d)", c.MidAttempts)
	}
	if c.SlowAttempts < 0 {
		return fmt.Errorf("BackoffConfig.SlowAttempts must be >= 0 (got %d)", c.SlowAttempts)
	}
	if c.MidInterval < c.InitialInterval {
		return fmt.Errorf("BackoffConfig.MidInterval (%v) < InitialInterval (%v); tiers must be monotonically non-decreasing", c.MidInterval, c.InitialInterval)
	}
	if c.SlowInterval < c.MidInterval {
		return fmt.Errorf("BackoffConfig.SlowInterval (%v) < MidInterval (%v); tiers must be monotonically non-decreasing", c.SlowInterval, c.MidInterval)
	}
	if c.MaxInterval < c.SlowInterval {
		return fmt.Errorf("BackoffConfig.MaxInterval (%v) < SlowInterval (%v); cap below slow tier defeats the ramp", c.MaxInterval, c.SlowInterval)
	}
	return nil
}

// WithBackoff sets the reconnect backoff configuration. Invalid configs are
// rejected at option-application time: a Warn is logged and the default is
// kept. Callers wanting hard validation can call cfg.Validate() before
// passing.
func WithBackoff(cfg BackoffConfig) SessionOption {
	return func(s *Session) {
		if err := cfg.Validate(); err != nil {
			if s.logger != nil {
				s.logger.Warn("WithBackoff: invalid config, keeping current value",
					"error", err)
			}
			return
		}
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

// TargetCheck selects what NewSession does when the device at the target IP
// reports a different NetID than the caller supplied. Set with WithTargetCheck.
type TargetCheck int

const (
	// TargetCheckWarn logs a warning and continues. Default, because a mismatch
	// is not proof of a mistake: pointed at a router, the NetID you want
	// legitimately belongs to a device behind it.
	TargetCheckWarn TargetCheck = iota + 1
	// TargetCheckError refuses to construct the session. Right for a deployment
	// that talks straight to its PLCs, where a mismatch is always a
	// misconfiguration and failing at startup beats a session that connects and
	// then answers nothing.
	TargetCheckError
	// TargetCheckOff skips the check, and with it the one UDP round-trip it
	// costs. Also the way to stay silent on a host that is deliberately
	// addressed through a router.
	TargetCheckOff
)

// WithTargetCheck sets what happens when the target NetID disagrees with what the
// device reports. Default TargetCheckWarn. Runs in Connect for one UDP round-trip,
// and only for a caller-supplied target -- an incomplete one is resolved from the
// device, which is authoritative. A device that does not answer identify is never
// a mismatch in any mode: a firewalled UDP port says nothing about the address.
func WithTargetCheck(c TargetCheck) SessionOption {
	return func(s *Session) {
		if c != 0 {
			s.targetCheck = c
		}
	}
}

// WithRouteActivationTimeout caps how long Connect waits for a freshly registered
// route to actually be served: the router acks the registration before the entry
// is live, and until then requests are dropped with no reply. Default 10s covers
// every PLC observed. Values <= 0 are ignored, and a deadline on Connect's context
// also bounds the wait, so this is only needed to wait longer.
func WithRouteActivationTimeout(d time.Duration) SessionOption {
	return func(s *Session) {
		if d > 0 {
			s.route.activationTimeout = d
		}
	}
}

// WithAmsPeerListen makes the session listen for a connection the PLC opens back
// to us and use it for responses. Needed for devices that treat a registered route
// as a peer router: they process our requests on our connection but answer over
// one they open to us. Without it such a device looks like it times out on
// everything, because the responses reach a socket nobody is listening on.
//
// port is normally 48898, parameterised for tests and hosts already running a
// TwinCAT router. Off by default, since binding is not a client library's business
// unless asked; binding failures surface from Connect rather than silently.
func WithAmsPeerListen(port int) SessionOption {
	return func(s *Session) {
		s.peerListenPort = port
	}
}

// WithoutAmsPeerFallback disables the automatic peer-listener fallback, which
// otherwise binds the AMS port when a Connect proves the PLC answers nothing at
// all (see WithAmsPeerListen). The fallback only binds for a session that would
// be dead anyway. Use this where a TwinCAT router already owns the port, or where
// a client process may not listen.
func WithoutAmsPeerFallback() SessionOption {
	return func(s *Session) {
		s.peerFallbackDisabled = true
	}
}

// WithForceRouteRegistration skips probing and registers on every Connect and
// Reconnect, for environments where routes are not persistent. The cost is one UDP
// registration per attempt on a flapping link, against a router this library has
// seen go mute under duplicate entries -- freshness traded for route-table safety.
// Without it a session registers once, plus one healing registration per cooldown.
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

// WithSymbolVersionStrategy selects the online-change handling strategy.
// Default: SymbolVersionAutoReload (zero-value, applies if option not set).
//
// Values outside the SymbolVersionAutoReload/Close/Ignore enumeration are
// rejected at option-application time: a warning is logged and the strategy
// falls back to AutoReload. The strategy controls what handleStaleDetection
// does when a stale-cache return code (0x711, 0x705, 0x710, 0x704, 0x703,
// 0x702) is observed.
func WithSymbolVersionStrategy(s SymbolVersionStrategy) SessionOption {
	return func(sess *Session) {
		switch s {
		case SymbolVersionAutoReload, SymbolVersionClose, SymbolVersionIgnore:
			sess.versionStrategy = s
		default:
			if sess.logger != nil {
				sess.logger.Warn("invalid SymbolVersionStrategy, using AutoReload",
					"got", uint8(s))
			}
			sess.versionStrategy = SymbolVersionAutoReload
		}
	}
}

// WithMaxSymbolVersionReloadAttempts caps reload attempts under
// SymbolVersionAutoReload within a sliding window. Default: 3. n<1 is
// rejected (logged Warn, default kept).
//
// Note: unlike WithMaxReconnectAttempts (where n=0 means infinite), reload
// attempts are intentionally bounded — runaway reload loops would hammer
// the PLC under recurring online-change conditions.
func WithMaxSymbolVersionReloadAttempts(n int) SessionOption {
	return func(sess *Session) {
		if n < 1 {
			if sess.logger != nil {
				sess.logger.Warn("WithMaxSymbolVersionReloadAttempts: n<1 rejected, keeping current value", "n", n)
			}
			return
		}
		sess.maxReloadAttempts = n
	}
}

// WithSymbolVersionReloadWindow sets the sliding window for reload-attempt
// counting. Default: 60s. d<=0 is rejected (logged Warn, default kept).
func WithSymbolVersionReloadWindow(d time.Duration) SessionOption {
	return func(sess *Session) {
		if d <= 0 {
			if sess.logger != nil {
				sess.logger.Warn("WithSymbolVersionReloadWindow: d<=0 rejected, keeping current value", "d", d)
			}
			return
		}
		sess.reloadWindow = d
	}
}

// WithOnSymbolVersionChanged registers a callback fired once per stale-cache
// detection. The reason argument is one of the documented enumerated values
// on Update.Reason (symbol-version-invalid, invalid-size, ...). Callback
// runs in its own goroutine — do NOT block.
//
// Under SymbolVersionIgnore strategy this callback is the only signal for
// symbol-removed events: the dead handle's user channel goes silent (no
// terminal Update). Surviving sibling handles still receive a one-shot
// Stale=true Update; only the removed symbol's channel is mute.
func WithOnSymbolVersionChanged(fn func(reason Reason)) SessionOption {
	return func(sess *Session) {
		sess.versionCallback = fn
	}
}

// WithNotificationHeartbeat tunes the internal cyclic notification that detects
// subscriptions dying silently. A subscription can stop delivering with nothing
// observable happening, and an on-change subscription may be silent legitimately,
// so only a cyclic beat's absence is conclusive.
//
// interval is the cycle time; missed is how many beats may be lost before the
// session re-subscribes. Defaults 2s and 5 (~10s to notice); missed < 2 is raised
// to 2. See WithNotificationSilenceTimeout for a duration instead of a count,
// WithHeartbeatRecovery for what happens next, and WithRuntimeStateWatch for the
// runtime-state poll, which this no longer affects.
func WithNotificationHeartbeat(interval time.Duration, missed int) SessionOption {
	return func(s *Session) {
		if interval > 0 {
			// ADS carries cycle times as 32-bit 100ns ticks, so anything beyond
			// ~429s cannot be expressed and the subscription would be rejected —
			// leaving the session with no heartbeat at all, which is worse than a
			// slow one. Clamp rather than fail.
			if interval > maxADSCycleTime {
				if s.logger != nil {
					s.logger.Warn("heartbeat interval exceeds what ADS can express; clamping",
						"requested", interval, "using", maxADSCycleTime)
				}
				interval = maxADSCycleTime
			}
			s.heartbeatInterval = interval
		}
		if missed < 2 {
			missed = 2
		}
		s.heartbeatMissed = missed
		// Clearing the duration form is what makes the two options last-wins: the
		// caller stated the tick count later, so it is the one that should decide,
		// and normalizeHeartbeatOptions only converts a duration that is still set.
		s.heartbeatSilence = 0
		s.heartbeatDisabled = false
	}
}

// WithNotificationSilenceTimeout says how long subscriptions may be silent before
// the session calls them dead, in wall-clock time -- the same decision as
// WithNotificationHeartbeat's missed argument, in the unit an operator thinks in.
// missed is derived from it at construction (rounded up, floored at 2), and
// whichever of the two options is applied later wins.
func WithNotificationSilenceTimeout(d time.Duration) SessionOption {
	return func(s *Session) {
		if d <= 0 {
			return
		}
		s.heartbeatSilence = d
		s.heartbeatDisabled = false
	}
}

// WithHeartbeatRecovery selects what happens when the heartbeat goes silent. The
// default, Immediate, re-subscribes at once -- a delete plus an add per handle, 82
// requests on a 41-symbol session, against a device that may have merely stalled.
// Confirm waits for a second silent window, doubling the time to notice but
// halving needless churn. Observe never re-subscribes and reports it instead. An
// unrecognised value keeps the default: a typo must not turn recovery off.
func WithHeartbeatRecovery(mode HeartbeatRecovery) SessionOption {
	return func(s *Session) {
		switch mode {
		case HeartbeatRecoveryImmediate, HeartbeatRecoveryConfirm, HeartbeatRecoveryObserve:
			s.heartbeatRecovery = mode
		default:
			if s.logger != nil {
				s.logger.Warn("WithHeartbeatRecovery: unrecognised mode, keeping the default",
					"mode", int(mode), "using", HeartbeatRecoveryImmediate.String())
			}
		}
	}
}

// WithRuntimeStateWatch sets how often the session polls the system service for
// the runtime state. That reading is what lets symbol and subscription calls
// refuse with "the runtime is not running" instead of failing obscurely, and lets
// a session starting in CONFIG come up and wait. Default 5s, independent of the
// heartbeat interval.
func WithRuntimeStateWatch(d time.Duration) SessionOption {
	return func(s *Session) {
		if d <= 0 {
			return
		}
		s.stateWatchInterval = d
		s.stateWatchDisabled = false
	}
}

// WithoutRuntimeStateWatch turns the runtime-state poll off, saving one small
// request per interval. In exchange the gates fall back to permitting, so a PLC in
// CONFIG fails the old obscure way instead of saying the runtime is not running,
// and a session starting in CONFIG will not notice the return to RUN. Connect
// still does one synchronous read.
func WithoutRuntimeStateWatch() SessionOption {
	return func(s *Session) {
		s.stateWatchDisabled = true
	}
}

// WithoutNotificationHeartbeat disables the heartbeat described in
// WithNotificationHeartbeat.
//
// The cost it saves: one notification handle in the PLC's table per session, and
// one small cyclic sample per interval. The cost it accepts: a runtime restart or
// CONFIG toggle leaves this session's subscriptions dead permanently, with no
// error and nothing in the session's state to show it — the consumer has to notice
// the absence of data and rebuild the session itself.
func WithoutNotificationHeartbeat() SessionOption {
	return func(s *Session) {
		s.heartbeatDisabled = true
	}
}
