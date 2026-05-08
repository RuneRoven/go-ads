package ads

import (
	"sync"

	"go.uber.org/atomic"
)

// notificationManager owns the connection-level notification state:
// the per-handle Symbol map, the saved configs for reconnect re-subscribe,
// the user-supplied channel that all notifications are dispatched to, and
// the timestamp of the most recent successful subscribe (used to suppress
// "unknown handle" warnings during the first-sample race window).
//
// Lock ordering: NEVER hold both cache.lock and notifications.lock simultaneously.
type notificationManager struct {
	lock                sync.Mutex
	activeNotifications map[uint32]*Symbol
	notificationConfigs []NotificationConfig
	notificationChannel chan *Update
	lastSubscribeNs     atomic.Int64
}
