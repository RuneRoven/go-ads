package ads

import (
	"testing"
)

// bestEffortDeleteNotifications returns 0 for an empty input slice and never
// touches the network.
func TestBestEffortDeleteNotifications_Empty(t *testing.T) {
	conn := &Connection{logger: getDefaultLogger()}
	got := conn.bestEffortDeleteNotifications(nil)
	if got != 0 {
		t.Errorf("expected 0, got %d", got)
	}
	got = conn.bestEffortDeleteNotifications([]uint32{})
	if got != 0 {
		t.Errorf("expected 0, got %d", got)
	}
}
