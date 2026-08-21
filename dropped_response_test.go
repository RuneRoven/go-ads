package ads

import (
	"errors"
	"log/slog"
	"testing"
	"time"
)

// TestDroppedDoesNotDiscardArrivedReply: a PLC that answers and then closes must
// still yield its answer. listen() queues the frame for a recvWorker but closes
// the dropped channel on its own goroutine immediately, so the drop signal can
// overtake the response and sendRequest can return ErrTransportClosed for a
// request that WAS answered.
func TestDroppedDoesNotDiscardArrivedReply(t *testing.T) {
	const runs = 40
	lost, got := 0, 0
	for i := 0; i < runs; i++ {
		srv := startScriptableServer(t)
		srv.onRead(GroupSymbolVersion, func(_, _, _ uint32) (ReturnCode, []byte) {
			return ReturnCodeNoErrors, []byte{42}
		})
		srv.answerThenClose(CommandIDRead, 1)

		c, err := Dial(srv.host, srv.port, AMSAddress{}, AMSAddress{}, 2*time.Second,
			WithClientLogger(slog.New(&testLogHandler{})))
		if err != nil {
			t.Fatalf("Dial: %v", err)
		}
		v, err := c.GetSymbolVersion(t.Context())
		switch {
		case err == nil && v == 42:
			got++
		case errors.Is(err, ErrTransportClosed):
			lost++
		default:
			t.Logf("run %d: unexpected outcome v=%d err=%v", i, v, err)
		}
		_ = c.Close()
		srv.stop()
	}
	t.Logf("answered-then-closed: %d/%d replies delivered, %d lost to ErrTransportClosed", got, runs, lost)
	if lost > 0 {
		t.Errorf("%d of %d replies were discarded despite having arrived", lost, runs)
	}
}
