package ads

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"net"
	"os"
	"testing"
	"time"
)

// How many notification samples per second the session turns into Updates,
// end to end over a real socket. Set ADS_THROUGHPUT=1 to run it.
func TestNotificationThroughput(t *testing.T) {
	if os.Getenv("ADS_THROUGHPUT") == "" {
		t.Skip("ADS_THROUGHPUT not set")
	}
	srv := startScriptableServer(t)
	defer srv.stop()
	sess, c := newWiredTestSession(t, srv, WithoutNotificationHeartbeat())
	c.SetNotificationHandler(sess.handleNotification)
	const handles = 1000
	ch := make(chan *Update, 1<<16)
	sess.notifications.lock.Lock()
	for h := uint32(1); h <= handles; h++ {
		sym := preSeedTypedSymbol(sess, fmt.Sprintf("MAIN.n%d", h), h)
		sym.DataType, sym.Length = "DINT", 4
		sess.notifications.activeNotifications[h] = activeNotification{Sym: sym, Ch: ch}
	}
	sess.notifications.lock.Unlock()

	// One frame per PLC cycle: one stamp carrying a sample for every handle.
	frame := func(cycle uint64) []byte {
		var p bytes.Buffer
		_ = binary.Write(&p, binary.LittleEndian, uint32(0))
		_ = binary.Write(&p, binary.LittleEndian, uint32(1))
		_ = binary.Write(&p, binary.LittleEndian, cycle)
		_ = binary.Write(&p, binary.LittleEndian, uint32(handles))
		for h := uint32(1); h <= handles; h++ {
			_ = binary.Write(&p, binary.LittleEndian, h)
			_ = binary.Write(&p, binary.LittleEndian, uint32(4))
			_ = binary.Write(&p, binary.LittleEndian, uint32(cycle))
		}
		return notificationFrame(p.Bytes())
	}
	srv.mu.Lock()
	var conn net.Conn
	for c := range srv.conns {
		conn = c
	}
	srv.mu.Unlock()

	const cycles = 300
	frames := make([][]byte, cycles)
	for i := range frames {
		frames[i] = frame(uint64(i + 1))
	}
	got := 0
	var last time.Time
	done := make(chan struct{})
	go func() {
		defer close(done)
		for got < cycles*handles {
			select {
			case <-ch:
				got++
				last = time.Now()
			case <-time.After(2 * time.Second):
				return
			}
		}
	}()
	t0 := time.Now()
	for _, f := range frames {
		if _, err := conn.Write(f); err != nil {
			t.Fatalf("write: %v", err)
		}
	}
	<-done
	el := last.Sub(t0)
	t.Logf("THROUGHPUT %d of %d samples delivered in %s: %.0f samples/s", got, cycles*handles, el.Round(time.Millisecond), float64(got)/el.Seconds())
}
