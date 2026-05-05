package ads

import (
	"context"
	"testing"
	"time"
)

// Verify that sending to a closed user channel does NOT panic the listen goroutine.
// Go runtime panics on send-to-closed-channel regardless of select default,
// so deliverNotification must guard with defer recover().
func TestDeliverNotification_ClosedChannelDoesNotPanic(t *testing.T) {
	ch := make(chan *Update, 1)
	close(ch)

	defer func() {
		if r := recover(); r != nil {
			t.Errorf("expected no panic, got recovered panic: %v", r)
		}
	}()

	conn := &Connection{logger: getDefaultLogger()}
	ctx := context.Background()
	update := &Update{Variable: "x", Value: "1", TimeStamp: time.Now()}

	conn.deliverNotification(ctx, ch, update, 42, "x")
}

// Verify normal happy-path delivery on an open buffered channel.
func TestDeliverNotification_DeliversOnOpenChannel(t *testing.T) {
	ch := make(chan *Update, 1)
	conn := &Connection{logger: getDefaultLogger()}
	ctx := context.Background()
	update := &Update{Variable: "x", Value: "1", TimeStamp: time.Now()}

	conn.deliverNotification(ctx, ch, update, 42, "x")

	select {
	case got := <-ch:
		if got != update {
			t.Errorf("got %v, want %v", got, update)
		}
	default:
		t.Errorf("update was not delivered to channel")
	}
}

// Verify drop on full buffered channel (default branch of select fires).
func TestDeliverNotification_DropsWhenChannelFull(t *testing.T) {
	ch := make(chan *Update, 1)
	ch <- &Update{Variable: "filler"} // fill buffer

	defer func() {
		if r := recover(); r != nil {
			t.Errorf("expected no panic, got recovered panic: %v", r)
		}
	}()

	conn := &Connection{logger: getDefaultLogger()}
	ctx := context.Background()
	update := &Update{Variable: "x", Value: "1", TimeStamp: time.Now()}

	conn.deliverNotification(ctx, ch, update, 42, "x")
	// Should drop without panic; channel still has only the filler.
	if len(ch) != 1 {
		t.Errorf("expected channel to keep filler, got len=%d", len(ch))
	}
}
