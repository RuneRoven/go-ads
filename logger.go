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

// defaultLoggerPtr is used by package-level functions (e.g., AddRemoteRoute)
// that are not associated with a Session instance.
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
