package ads

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// scriptableServer is a programmable wire-level ADS PLC stub.
//
// Unlike echoServer (client_test.go), which always returns an empty Read
// response with InvokeID echoed, scriptableServer dispatches each inbound
// AMS frame to a per-test handler keyed by (CommandID, group). Tests
// register handlers per command (or per WriteRead group) and the server
// constructs a canonical response from the handler's return value.
//
// Default behavior: any (cmd, group) pair without a registered handler
// returns ReturnCodeDeviceServiceNotSupported. Inbound frames are recorded
// for post-test assertions.
//
// All registration helpers are concurrency-safe and may be called BEFORE
// or AFTER startScriptableServer() returns; the per-conn handle goroutine
// looks up handlers under the server's mutex on every request.

// addNotifRequest mirrors the wire-level AddDeviceNotification payload.
type addNotifRequest struct {
	Group     uint32
	Offset    uint32
	Length    uint32
	TransMode uint32
	MaxDelay  uint32 // 100ns units
	CycleTime uint32 // 100ns units
}

// addNotifResponse is what the test handler returns for AddDeviceNotification.
type addNotifResponse struct {
	Handle uint32
	Error  ReturnCode
}

// sumNotifResponse — per-item response for SumAddDeviceNotification.
type sumNotifResponse struct {
	Error  ReturnCode
	Handle uint32
}

type (
	writeReadHandler func(req []byte) []byte
	writeHandler     func(group, offset uint32, data []byte) ReturnCode
	readHandler      func(group, offset, length uint32) (ReturnCode, []byte)
)

type scriptableServer struct {
	host string
	port int
	ln   net.Listener
	t    *testing.T
	wg   sync.WaitGroup

	mu sync.Mutex // guards every field below

	// Per-cmd dispatch tables. key (group) used for Read/Write/WriteRead.
	writeReadHandlers map[uint32]writeReadHandler
	writeHandlers     map[uint32]writeHandler
	readHandlers      map[uint32]readHandler

	addNotifFn    func(req addNotifRequest) addNotifResponse
	deleteNotifFn func(handle uint32) ReturnCode

	// Optional artificial latency, keyed by (cmd, group). For commands that
	// don't carry a group (AddDeviceNotification etc.) the group is 0.
	delays map[delayKey]time.Duration

	// dropAfter/dropSeen implement dropConnAfter: close the connection instead
	// of answering the nth occurrence of a command.
	dropAfter map[CommandID]int
	dropSeen  map[CommandID]int

	// closeAfterReply implements answerThenClose: answer the nth occurrence of a
	// command normally, then close the connection. Models the PLC behaviour that
	// matters most here — a reply followed immediately by a route-idle close, a
	// runtime restart, or an RST.
	closeAfterReply map[CommandID]int
	replySeen       map[CommandID]int

	// Recorded inbound frames (full bytes including TCP header).
	frameBuf [][]byte

	// acceptCount is outside mu on purpose: a reconnect-storm test reads it while
	// the accept loop is running.
	acceptCount atomic.Int64

	// peerAddr, when set, makes the stub answer on a connection IT opens to that
	// address instead of on the client's connection — the behaviour measured on a
	// TC/RTOS device, which treats a registered route as a peer router. Requests
	// are still accepted on the client's connection.
	peerAddr atomic.Pointer[string]
	peerMu   sync.Mutex
	peerConn net.Conn
}

type delayKey struct {
	cmd   CommandID
	group uint32 // 0 for non-group cmds
}

func startScriptableServer(t *testing.T) *scriptableServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr, ok := ln.Addr().(*net.TCPAddr)
	if !ok {
		_ = ln.Close()
		t.Fatalf("unexpected addr type: %T", ln.Addr())
	}
	s := &scriptableServer{
		host:              addr.IP.String(),
		port:              addr.Port,
		ln:                ln,
		t:                 t,
		writeReadHandlers: map[uint32]writeReadHandler{},
		writeHandlers:     map[uint32]writeHandler{},
		readHandlers:      map[uint32]readHandler{},
		delays:            map[delayKey]time.Duration{},
	}
	s.wg.Add(1)
	go s.acceptLoop()
	return s
}

func (s *scriptableServer) stop() {
	s.peerMu.Lock()
	if s.peerConn != nil {
		_ = s.peerConn.Close()
		s.peerConn = nil
	}
	s.peerMu.Unlock()
	_ = s.ln.Close()
	done := make(chan struct{})
	go func() { s.wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
	}
}

// onWriteRead registers a handler for the given group on CommandIDReadWrite.
func (s *scriptableServer) onWriteRead(group Group, fn writeReadHandler) {
	s.mu.Lock()
	s.writeReadHandlers[uint32(group)] = fn
	s.mu.Unlock()
}

// onWrite registers a handler for the given group on CommandIDWrite.
func (s *scriptableServer) onWrite(group Group, fn writeHandler) {
	s.mu.Lock()
	s.writeHandlers[uint32(group)] = fn
	s.mu.Unlock()
}

// onRead registers a handler for the given group on CommandIDRead.
func (s *scriptableServer) onRead(group Group, fn readHandler) {
	s.mu.Lock()
	s.readHandlers[uint32(group)] = fn
	s.mu.Unlock()
}

// onAddDeviceNotification registers the AddDeviceNotification handler.
func (s *scriptableServer) onAddDeviceNotification(fn func(req addNotifRequest) addNotifResponse) {
	s.mu.Lock()
	s.addNotifFn = fn
	s.mu.Unlock()
}

// onDeleteDeviceNotification registers the DeleteDeviceNotification handler.
func (s *scriptableServer) onDeleteDeviceNotification(fn func(handle uint32) ReturnCode) {
	s.mu.Lock()
	s.deleteNotifFn = fn
	s.mu.Unlock()
}

// delayBefore injects artificial latency for a particular (cmd, group)
// before the handler runs. Pass group=0 for commands without an index group
// (AddDeviceNotification, DeleteDeviceNotification, ReadDeviceInfo, ReadState).
func (s *scriptableServer) delayBefore(cmd CommandID, group uint32, d time.Duration) {
	s.mu.Lock()
	s.delays[delayKey{cmd: cmd, group: group}] = d
	s.mu.Unlock()
}

// frames returns a snapshot of every fully-received inbound frame.
func (s *scriptableServer) frames() [][]byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([][]byte, len(s.frameBuf))
	copy(out, s.frameBuf)
	return out
}

// accepts reports how many TCP connections the stub has accepted. A reconnect
// loop that keeps dialing shows up here even when it never gets a reply.
func (s *scriptableServer) accepts() int {
	return int(s.acceptCount.Load())
}

func (s *scriptableServer) acceptLoop() {
	defer s.wg.Done()
	for {
		c, err := s.ln.Accept()
		if err != nil {
			return
		}
		s.acceptCount.Add(1)
		s.wg.Add(1)
		go s.handle(c)
	}
}

func (s *scriptableServer) handle(c net.Conn) {
	defer s.wg.Done()
	defer c.Close()
	for {
		hdr := make([]byte, 6)
		if _, err := io.ReadFull(c, hdr); err != nil {
			return
		}
		bodyLen := binary.LittleEndian.Uint32(hdr[2:6])
		if bodyLen > 4*1024*1024 {
			return
		}
		body := make([]byte, bodyLen)
		if _, err := io.ReadFull(c, body); err != nil {
			return
		}
		if len(body) < 32 {
			return
		}
		frame := append(append([]byte{}, hdr...), body...)
		s.mu.Lock()
		s.frameBuf = append(s.frameBuf, frame)
		s.mu.Unlock()

		cmd := CommandID(binary.LittleEndian.Uint16(body[16:18]))
		invokeID := binary.LittleEndian.Uint32(body[28:32])
		payload := body[32:] // ADS request payload

		// Decode group for cmds that carry one. Read/Write/WriteRead all
		// start with Group(4) Offset(4) ...
		var group uint32
		switch cmd {
		case CommandIDRead, CommandIDWrite, CommandIDReadWrite:
			if len(payload) >= 4 {
				group = binary.LittleEndian.Uint32(payload[0:4])
			}
		}

		// Honor any registered delay before dispatching.
		s.mu.Lock()
		if d := s.delays[delayKey{cmd: cmd, group: group}]; d > 0 {
			s.mu.Unlock()
			time.Sleep(d)
		} else {
			s.mu.Unlock()
		}

		// Drop the connection instead of answering, once the configured number
		// of this command has been seen. Models a PLC/link failure landing
		// mid-operation, which no amount of canned error codes reproduces: the
		// client sees EOF on a request it already sent.
		s.mu.Lock()
		dropAt, armed := s.dropAfter[cmd]
		if armed {
			s.dropSeen[cmd]++
			if s.dropSeen[cmd] >= dropAt {
				delete(s.dropAfter, cmd)
				s.mu.Unlock()
				return // deferred c.Close() drops it
			}
		}
		s.mu.Unlock()

		respPayload := s.dispatch(cmd, group, payload)
		out, werr := s.responseWriter(c)
		if werr != nil {
			// Faithful to the measured device: the request was accepted and
			// processed, the answer simply goes somewhere we cannot reach right
			// now. The client connection stays OPEN — closing it would look like a
			// transport drop, which is a different failure entirely.
			s.t.Logf("stub: no peer connection for the response yet (%v); leaving the request unanswered", werr)
			continue
		}
		if err := writeResponse(out, body, cmd, invokeID, respPayload); err != nil {
			return
		}

		// Answer-then-close: the reply is already on the wire; dropping the
		// connection now races the client's own response delivery.
		s.mu.Lock()
		closeAt, armed2 := s.closeAfterReply[cmd]
		if armed2 {
			s.replySeen[cmd]++
			if s.replySeen[cmd] >= closeAt {
				delete(s.closeAfterReply, cmd)
				s.mu.Unlock()
				return // deferred c.Close()
			}
		}
		s.mu.Unlock()
	}
}

// answerViaPeerConnection makes the stub deliver every response over a connection
// it opens to addr, leaving the client's own connection silent.
func (s *scriptableServer) answerViaPeerConnection(addr string) {
	a := addr
	s.peerAddr.Store(&a)
}

// responseWriter returns where a response should be written: the client's
// connection normally, or the stub's own connection to the client when
// answerViaPeerConnection is armed.
func (s *scriptableServer) responseWriter(client net.Conn) (net.Conn, error) {
	addr := s.peerAddr.Load()
	if addr == nil {
		return client, nil
	}
	s.peerMu.Lock()
	defer s.peerMu.Unlock()
	if s.peerConn != nil {
		return s.peerConn, nil
	}
	c, err := net.DialTimeout("tcp4", *addr, 2*time.Second)
	if err != nil {
		return nil, err
	}
	s.peerConn = c
	return c, nil
}

// answerThenClose answers the nth occurrence of cmd (1-based) and then closes
// the connection, then disarms.
func (s *scriptableServer) answerThenClose(cmd CommandID, n int) {
	s.mu.Lock()
	if s.closeAfterReply == nil {
		s.closeAfterReply = map[CommandID]int{}
		s.replySeen = map[CommandID]int{}
	}
	s.closeAfterReply[cmd] = n
	s.replySeen[cmd] = 0
	s.mu.Unlock()
}

// dropConnAfter closes the connection without answering the nth occurrence of
// cmd (1-based), then disarms. Use to land a transport failure in the middle of
// a multi-request operation.
func (s *scriptableServer) dropConnAfter(cmd CommandID, n int) {
	s.mu.Lock()
	if s.dropAfter == nil {
		s.dropAfter = map[CommandID]int{}
		s.dropSeen = map[CommandID]int{}
	}
	s.dropAfter[cmd] = n
	s.dropSeen[cmd] = 0
	s.mu.Unlock()
}

func (s *scriptableServer) dispatch(cmd CommandID, group uint32, payload []byte) []byte {
	s.mu.Lock()
	defer s.mu.Unlock()

	switch cmd {
	case CommandIDReadWrite:
		// payload: Group(4) Offset(4) ReadLen(4) WriteLen(4) Data...
		if fn := s.writeReadHandlers[group]; fn != nil {
			if len(payload) < 16 {
				return respondErrorBytes(ReturnCodeDeviceInvalidSize)
			}
			writeLen := binary.LittleEndian.Uint32(payload[12:16])
			if uint64(16+writeLen) > uint64(len(payload)) {
				return respondErrorBytes(ReturnCodeDeviceInvalidSize)
			}
			req := payload[16 : 16+writeLen]
			data := fn(req)
			return buildReadResponse(ReturnCodeNoErrors, data)
		}
		return respondErrorBytes(ReturnCodeDeviceServiceNotSupported)

	case CommandIDRead:
		// payload: Group(4) Offset(4) Length(4)
		if fn := s.readHandlers[group]; fn != nil {
			if len(payload) < 12 {
				return respondErrorBytes(ReturnCodeDeviceInvalidSize)
			}
			offset := binary.LittleEndian.Uint32(payload[4:8])
			length := binary.LittleEndian.Uint32(payload[8:12])
			rc, data := fn(group, offset, length)
			return buildReadResponse(rc, data)
		}
		return respondErrorBytes(ReturnCodeDeviceServiceNotSupported)

	case CommandIDWrite:
		// payload: Group(4) Offset(4) Length(4) Data...
		if fn := s.writeHandlers[group]; fn != nil {
			if len(payload) < 12 {
				return respondErrorBytes(ReturnCodeDeviceInvalidSize)
			}
			offset := binary.LittleEndian.Uint32(payload[4:8])
			length := binary.LittleEndian.Uint32(payload[8:12])
			data := []byte{}
			if uint64(12+length) <= uint64(len(payload)) {
				data = payload[12 : 12+length]
			}
			rc := fn(group, offset, data)
			return respondErrorBytes(rc)
		}
		return respondErrorBytes(ReturnCodeDeviceServiceNotSupported)

	case CommandIDAddDeviceNotification:
		if s.addNotifFn != nil {
			req := decodeAddNotifRequest(payload)
			r := s.addNotifFn(req)
			return buildAddNotifResponse(r.Handle, r.Error)
		}
		return buildAddNotifResponse(0, ReturnCodeDeviceServiceNotSupported)

	case CommandIDDeleteDeviceNotification:
		if s.deleteNotifFn != nil {
			if len(payload) < 4 {
				return respondErrorBytes(ReturnCodeDeviceInvalidSize)
			}
			h := binary.LittleEndian.Uint32(payload[0:4])
			rc := s.deleteNotifFn(h)
			return respondErrorBytes(rc)
		}
		return respondErrorBytes(ReturnCodeDeviceServiceNotSupported)

	case CommandIDReadDeviceInfo:
		// 24-byte canned response: 4 errCode + 1 major + 1 minor + 2 version + 16 name
		out := make([]byte, 24)
		copy(out[8:24], []byte("scriptableStub\x00\x00"))
		return out

	case CommandIDReadState:
		// 8 bytes: 4 errCode + 2 ADSState + 2 DeviceState
		out := make([]byte, 8)
		binary.LittleEndian.PutUint16(out[4:6], uint16(ADSStateRun))
		return out
	}
	return respondErrorBytes(ReturnCodeDeviceServiceNotSupported)
}

// --- response builders ---

// respondErrorBytes returns a 4-byte ReturnCode payload.
func respondErrorBytes(rc ReturnCode) []byte {
	out := make([]byte, 4)
	binary.LittleEndian.PutUint32(out, uint32(rc))
	return out
}

// buildReadResponse builds an 8-byte (errCode + length) header followed by data.
// Used for both CommandIDRead and CommandIDReadWrite.
func buildReadResponse(rc ReturnCode, data []byte) []byte {
	out := make([]byte, 8+len(data))
	binary.LittleEndian.PutUint32(out[0:4], uint32(rc))
	binary.LittleEndian.PutUint32(out[4:8], uint32(len(data)))
	copy(out[8:], data)
	return out
}

// buildAddNotifResponse builds the 8-byte AddDeviceNotification response payload.
func buildAddNotifResponse(handle uint32, rc ReturnCode) []byte {
	out := make([]byte, 8)
	binary.LittleEndian.PutUint32(out[0:4], uint32(rc))
	binary.LittleEndian.PutUint32(out[4:8], handle)
	return out
}

// buildSumAddNotifPayload returns the data section for a SumAddDeviceNotification
// WriteRead response (per-item: 4 errCode + 4 handle).
func buildSumAddNotifPayload(items []sumNotifResponse) []byte {
	out := make([]byte, 8*len(items))
	for i, it := range items {
		binary.LittleEndian.PutUint32(out[i*8:i*8+4], uint32(it.Error))
		binary.LittleEndian.PutUint32(out[i*8+4:i*8+8], it.Handle)
	}
	return out
}

// buildSumDeleteNotifPayload returns the data section for a SumDeleteDeviceNotification
// WriteRead response (per-item: 4 errCode).
func buildSumDeleteNotifPayload(codes []ReturnCode) []byte {
	out := make([]byte, 4*len(codes))
	for i, c := range codes {
		binary.LittleEndian.PutUint32(out[i*4:i*4+4], uint32(c))
	}
	return out
}

// buildSymbolInfoPayload encodes a GetSymbolInfoByName response: symbolEntry
// struct followed by name+0, datatype+0, comment+0.
func buildSymbolInfoPayload(name, dataType, comment string, group, offset, size uint32, baseType ADSDataType, flags SymbolFlag) []byte {
	entry := symbolEntry{
		IGroup:        group,
		IOffs:         offset,
		Size:          size,
		DataType:      uint32(baseType),
		Flags:         uint32(flags),
		NameLength:    uint16(len(name)),
		TypeLength:    uint16(len(dataType)),
		CommentLength: uint16(len(comment)),
	}
	// EntryLength = sizeof(symbolEntry) + name+0 + dt+0 + comment+0
	entry.EntryLength = uint32(30 /*symbolEntry*/) + uint32(len(name)+1+len(dataType)+1+len(comment)+1)

	buf := new(bytes.Buffer)
	_ = binary.Write(buf, binary.LittleEndian, entry)
	buf.WriteString(name)
	buf.WriteByte(0)
	buf.WriteString(dataType)
	buf.WriteByte(0)
	buf.WriteString(comment)
	buf.WriteByte(0)
	return buf.Bytes()
}

// buildHandlePayload builds a 4-byte handle response (for GetHandleByName).
func buildHandlePayload(handle uint32) []byte {
	out := make([]byte, 4)
	binary.LittleEndian.PutUint32(out, handle)
	return out
}

// decodeAddNotifRequest parses the 40-byte request payload of AddDeviceNotification.
func decodeAddNotifRequest(payload []byte) addNotifRequest {
	var r addNotifRequest
	if len(payload) < 24 {
		return r
	}
	r.Group = binary.LittleEndian.Uint32(payload[0:4])
	r.Offset = binary.LittleEndian.Uint32(payload[4:8])
	r.Length = binary.LittleEndian.Uint32(payload[8:12])
	r.TransMode = binary.LittleEndian.Uint32(payload[12:16])
	r.MaxDelay = binary.LittleEndian.Uint32(payload[16:20])
	r.CycleTime = binary.LittleEndian.Uint32(payload[20:24])
	return r
}

// writeResponse builds and writes a complete response frame.
// reqBody is the original 32-byte AMS header (used to swap target/source).
// respPayload is the post-header response data.
func writeResponse(c net.Conn, reqBody []byte, cmd CommandID, invokeID uint32, respPayload []byte) error {
	respBody := make([]byte, 32+len(respPayload))
	// Swap source/target so addressing looks right.
	copy(respBody[0:8], reqBody[8:16]) // new Target = old Source
	copy(respBody[8:16], reqBody[0:8]) // new Source = old Target
	binary.LittleEndian.PutUint16(respBody[16:18], uint16(cmd))
	binary.LittleEndian.PutUint16(respBody[18:20], 5) // State = response
	binary.LittleEndian.PutUint32(respBody[20:24], uint32(len(respPayload)))
	binary.LittleEndian.PutUint32(respBody[24:28], 0) // ErrorCode
	binary.LittleEndian.PutUint32(respBody[28:32], invokeID)
	copy(respBody[32:], respPayload)

	respHdr := make([]byte, 6)
	binary.LittleEndian.PutUint32(respHdr[2:6], uint32(len(respBody)))

	if _, err := c.Write(append(respHdr, respBody...)); err != nil {
		return err
	}
	return nil
}

// --- Session wiring helper ---

// TestScriptableServer_Smoke validates the helper itself: register a
// Read handler, drive a single Read via *Client, and assert the canonical
// response builder + frame recording work end-to-end.
func TestScriptableServer_Smoke(t *testing.T) {
	srv := startScriptableServer(t)
	defer srv.stop()

	srv.onRead(GroupSymbolVersion, func(_, _, _ uint32) (ReturnCode, []byte) {
		return ReturnCodeNoErrors, []byte{0x42}
	})

	c, err := Dial(srv.host, srv.port, AMSAddress{}, AMSAddress{}, 2*time.Second)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer c.Close()

	got, err := c.Read(context.Background(), uint32(GroupSymbolVersion), 0, 1)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(got) != 1 || got[0] != 0x42 {
		t.Errorf("Read = %v, want [0x42]", got)
	}
	if len(srv.frames()) == 0 {
		t.Errorf("frames() = 0, want at least 1")
	}
}

// newWiredTestSession builds a Session whose .client is the *Client wired
// to srv. The FSM is transitioned to Connected so isClosed/isReconnecting
// short-circuits behave like a live session.
//
// Optional SessionOptions are applied after construction so tests can
// override defaults (e.g. WithSymbolVersionStrategy, WithOnDisconnect).
// lifecycle.ctx + lifecycle.shutdown are pre-initialised so sess.Close()
// is safe to call from tests; autoReconnect defaults to false to avoid
// spawning the Reconnect goroutine on disconnect.
//
// Caller is responsible for c.Close() at end of test (typically via t.Cleanup).
func newWiredTestSession(t *testing.T, srv *scriptableServer, opts ...SessionOption) (*Session, *Client) {
	t.Helper()
	c, err := Dial(srv.host, srv.port, AMSAddress{}, AMSAddress{}, 5*time.Second)
	if err != nil {
		t.Fatalf("Dial scriptable server: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })

	sess := &Session{
		tx:            c.tx,
		cache:         &symbolCache{symbols: map[string]*symbol{}, onDemandSymbols: map[string]bool{}},
		notifications: &notificationManager{activeNotifications: make(map[uint32]activeNotification), configsByKey: make(map[string]struct{}), orphanSeen: make(map[uint32]time.Time), orphanSem: make(chan struct{}, orphanDeleteMaxConcurrency)},
		lifecycle:     &sessionLifecycle{closedCh: make(chan struct{})},
		logger:        getDefaultLogger(),
	}
	sess.client.Store(c)
	// Pre-init ctx/shutdown so sess.Close() is safe in test paths that
	// exercise the close lifecycle (e.g. SymbolVersionClose strategy).
	// parentCtx too, matching NewSession: tearDownAndReset re-derives
	// lifecycle.ctx from it, so a helper that leaves it nil diverges from
	// production on every path that redials.
	sess.lifecycle.parentCtx = context.Background()
	sess.lifecycle.ctx, sess.lifecycle.shutdown = context.WithCancel(sess.lifecycle.parentCtx)
	// Walk the FSM through Connecting → Connected so isClosed/isReconnecting
	// readers see a live session. Direct atomic store keeps the test helper
	// simple — production transitions go through transitionTo.
	sess.lifecycle.state.value.Store(uint32(SessionStateConnected))
	for _, opt := range opts {
		opt(sess)
	}
	return sess, c
}
