package ads

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"math"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// Raw 0xF009 replies captured from a CX on TwinCAT 2.11.2234. For struct
// members and array elements NameLength counts the NUL terminator.
const (
	// .stMachineStatus.stMotor1.fSpeed: nameLen=33 for a 32-char name.
	tc211StructMemberReply = "4700000040400000c30f0000080000000500000002000000" +
		"2100050000002e53544d414348494e455354415455532e53544d4f544f52312e4653504545440000" +
		"4c5245414c0000"
	// .anCounters[1]: nameLen=15 for a 14-char name; the byte after it is '.', not NUL.
	tc211ArrayElementReply = "7100000040400000d7120000040000000300000001000000" +
		"0f0004003d002e414e434f554e544552535b315d002e44494e5400" +
		"205772697465206563686f202d20504c4320696e6372656d656e7473207768656e20616e79" +
		"207772697465207661726961626c65206368616e6765732000"
)

func mustHex(t *testing.T, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(s)
	if err != nil {
		t.Fatalf("bad fixture: %v", err)
	}
	return b
}

func TestCString(t *testing.T) {
	cases := []struct {
		in   []byte
		want string
	}{
		{[]byte("MAIN.x"), "MAIN.x"},
		{[]byte("MAIN.x\x00"), "MAIN.x"},
		{[]byte("MAIN.x\x00junk"), "MAIN.x"},
		{[]byte{0}, ""},
		{nil, ""},
	}
	for _, c := range cases {
		if got := cString(c.in); got != c.want {
			t.Errorf("cString(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

func TestGetSymbolInfoByName_TC211NulCountedInName(t *testing.T) {
	cases := []struct {
		reply, wantName, wantType string
	}{
		{tc211StructMemberReply, ".STMACHINESTATUS.STMOTOR1.FSPEED", "LREAL"},
		{tc211ArrayElementReply, ".ANCOUNTERS[1]", "DINT"},
	}
	for _, c := range cases {
		t.Run(c.wantName, func(t *testing.T) {
			srv := startScriptableServer(t)
			defer srv.stop()
			reply := mustHex(t, c.reply)
			srv.onWriteRead(GroupSymbolInfoByNameEx, func(_ []byte) []byte { return reply })
			_, client := newWiredTestSession(t, srv)

			sym, err := client.GetSymbolInfoByName(context.Background(), strings.ToLower(c.wantName))
			if err != nil {
				t.Fatalf("GetSymbolInfoByName: %v", err)
			}
			if sym.FullName != c.wantName || sym.Name != c.wantName {
				t.Errorf("FullName=%q Name=%q, want %q", sym.FullName, sym.Name, c.wantName)
			}
			if sym.DataType != c.wantType {
				t.Errorf("DataType = %q, want %q", sym.DataType, c.wantType)
			}
			if strings.ContainsRune(sym.Comment, 0) {
				t.Errorf("Comment %q contains NUL", sym.Comment)
			}
		})
	}
}

func TestParseUploadSymbolInfoSymbols_TC211NulCountedInName(t *testing.T) {
	data := append(mustHex(t, tc211StructMemberReply), mustHex(t, tc211ArrayElementReply)...)
	syms, err := parseUploadSymbolInfoSymbols(data, nil, nil)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	for _, name := range []string{".STMACHINESTATUS.STMOTOR1.FSPEED", ".ANCOUNTERS[1]"} {
		sym, ok := syms[symbolKey(name)]
		if !ok {
			t.Errorf("%q missing from parsed table (%d entries)", name, len(syms))
			continue
		}
		if sym.FullName != name {
			t.Errorf("FullName = %q, want %q", sym.FullName, name)
		}
	}
}

// The customer symptom: every sample dropped with "notification target
// symbol no longer in cache" because the cache key carried the NUL.
func TestAddSymbolNotification_TC211NulName_DeliversSamples(t *testing.T) {
	srv := startScriptableServer(t)
	defer srv.stop()
	reply := mustHex(t, tc211StructMemberReply)
	srv.onWriteRead(GroupSymbolInfoByNameEx, func(_ []byte) []byte { return reply })
	srv.onWriteRead(GroupSymbolHandleByName, func(_ []byte) []byte { return buildHandlePayload(7) })
	srv.onWrite(GroupSymbolReleaseHandle, func(_, _ uint32, _ []byte) ReturnCode { return ReturnCodeNoErrors })
	// Distinct handles: the heartbeat subscription takes one too.
	var nextHandle atomic.Uint32
	srv.onAddDeviceNotification(func(_ addNotifRequest) addNotifResponse {
		return addNotifResponse{Handle: 0x50 + nextHandle.Add(1), Error: ReturnCodeNoErrors}
	})
	srv.onDeleteDeviceNotification(func(_ uint32) ReturnCode { return ReturnCodeNoErrors })

	sess, client := newWiredTestSession(t, srv)
	client.SetNotificationHandler(sess.handleNotification)
	ctx := context.Background()

	ch := make(chan *Update, 1)
	notifHandle, err := sess.AddSymbolNotification(ctx, ".stMachineStatus.stMotor1.fSpeed", 0, 0, TransModeServerOnChange, ch)
	if err != nil {
		t.Fatalf("AddSymbolNotification: %v", err)
	}

	sample := make([]byte, 8)
	binary.LittleEndian.PutUint64(sample, math.Float64bits(12.5))
	if err := sess.drivePacket(ctx, buildNotificationPacket(notifHandle, 0, sample)); err != nil {
		t.Fatalf("drivePacket: %v", err)
	}
	select {
	case u := <-ch:
		if u.Variable != ".STMACHINESTATUS.STMOTOR1.FSPEED" {
			t.Errorf("Variable = %q", u.Variable)
		}
		if u.Value != "12.5" {
			t.Errorf("Value = %q, want 12.5", u.Value)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("sample dropped: no update delivered")
	}
}
