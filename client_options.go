package ads

import (
	"log/slog"
	"time"
)

// ClientOption configures optional construction parameters for Dial.
//
// Phase 5.a-types declares the type and three options. Phase 5.a-dial wires
// Dial to consume them.
type ClientOption func(*Client)

// WithClientLogger sets the slog.Logger for a Client. Nil is ignored.
func WithClientLogger(logger *slog.Logger) ClientOption {
	return func(c *Client) {
		if logger != nil {
			c.logger = logger
		}
	}
}

// WithClientRequestTimeout overrides the per-request and dial timeout.
// Values <= 0 are ignored (the default of 5s applies).
func WithClientRequestTimeout(d time.Duration) ClientOption {
	return func(c *Client) {
		if d > 0 {
			c.requestTimeout = d
		}
	}
}

// WithNotificationHandler installs a callback for inbound DeviceNotification
// packets. Session installs its own handler internally; raw Client consumers
// (CLI, web ADS browser) install their own to receive notifications, or
// leave nil to drop them silently.
func WithNotificationHandler(fn notificationHandler) ClientOption {
	return func(c *Client) {
		c.notify = fn
	}
}
