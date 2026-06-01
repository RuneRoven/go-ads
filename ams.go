package ads

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"strconv"
	"strings"
)

type amsTCPHeader struct {
	Unknown1 uint8
	System   uint8
	Length   uint32
}

// AMSHeader is the 32-byte ADS frame header (per Beckhoff AMS/ADS spec).
// Wire layout (little-endian):
//
//	offset 0  Target NetID(6) + Port(2)       = AMSAddress
//	offset 8  Source NetID(6) + Port(2)       = AMSAddress
//	offset 16 CommandID (2)
//	offset 18 State flags (2)
//	offset 20 Data length (4) — payload bytes following this header
//	offset 24 ErrorCode (4)
//	offset 28 InvokeID (4)
//
// Exported so router / proxy implementations can parse, mutate (e.g.
// rewrite Source for client-isolated routing), and re-encode frames
// without touching Client RPC plumbing. See ParseAMSHeader and
// EncodeAMSHeader for the wire codec.
type AMSHeader struct {
	Target    AMSAddress
	Source    AMSAddress
	Command   CommandID
	State     uint16
	Length    uint32
	ErrorCode uint32
	InvokeID  uint32
}

// AMSHeaderSize is the on-wire size of an AMSHeader in bytes.
const AMSHeaderSize = 32

// MaxAMSPayloadSize is a sanity bound on the Length field of a parsed
// AMSHeader. The Beckhoff ADS protocol does not define a hard ceiling but
// real-world frames stay under ~16MB (SumRead with thousands of items is
// the typical upper bound). 32MB is a defensive cap that rejects corrupt
// or hostile frames before they cause downstream slice-bounds panics in
// the documented "b[AMSHeaderSize : AMSHeaderSize+h.Length]" pattern.
const MaxAMSPayloadSize = 32 * 1024 * 1024 // 32 MiB

// ParseAMSHeader decodes a 32-byte AMS header from the front of b. The
// caller is responsible for stripping any preceding 6-byte AMS-over-TCP
// length prefix. Returns an error if b is shorter than AMSHeaderSize or
// if the decoded payload Length field exceeds MaxAMSPayloadSize (the
// latter catches corrupt/hostile frames that would otherwise panic the
// caller's "b[AMSHeaderSize : AMSHeaderSize+h.Length]" extraction).
// The payload (Length bytes) follows directly after the header in b;
// callers extract it as b[AMSHeaderSize : AMSHeaderSize+int(h.Length)].
func ParseAMSHeader(b []byte) (AMSHeader, error) {
	var h AMSHeader
	if len(b) < AMSHeaderSize {
		return h, fmt.Errorf("ams: header too short: %d bytes, need %d", len(b), AMSHeaderSize)
	}
	if err := binary.Read(bytes.NewReader(b[:AMSHeaderSize]), binary.LittleEndian, &h); err != nil {
		return h, fmt.Errorf("ams: parse header: %w", err)
	}
	if h.Length > MaxAMSPayloadSize {
		return h, fmt.Errorf("ams: declared payload length %d exceeds MaxAMSPayloadSize %d (corrupt or hostile frame)", h.Length, MaxAMSPayloadSize)
	}
	return h, nil
}

// EncodeAMSHeader serializes an AMSHeader to its 32-byte wire form.
// No length prefix is added; callers wrap with the 6-byte AMS-over-TCP
// header (1 unknown + 1 system + 4 length) when sending over TCP.
func EncodeAMSHeader(h AMSHeader) []byte {
	buf := make([]byte, 0, AMSHeaderSize)
	w := bytes.NewBuffer(buf)
	_ = binary.Write(w, binary.LittleEndian, h)
	return w.Bytes()
}

// ParseNetID converts a dotted-notation NetID string (e.g. "192.168.1.1.1.1")
// to a 6-byte array. Returns an error if the string is malformed (wrong
// number of parts or non-numeric values).
func ParseNetID(source string) (result [6]byte, err error) {
	parts := strings.Split(source, ".")
	if len(parts) != 6 {
		return result, fmt.Errorf("invalid NetID %q: expected 6 dot-separated parts, got %d", source, len(parts))
	}
	for i, a := range parts {
		value, e := strconv.ParseUint(a, 10, 8)
		if e != nil {
			return result, fmt.Errorf("invalid NetID %q: part %d (%q): %w", source, i, a, e)
		}
		result[i] = byte(value)
	}
	return
}

// NewAMSAddress parses a dotted-notation NetID string plus a uint16 port
// into an AMSAddress. The typical TwinCAT 3 PLC runtime listens on port
// 851 (PortR0PlcTc3).
func NewAMSAddress(netID string, port uint16) (AMSAddress, error) {
	id, err := ParseNetID(netID)
	if err != nil {
		return AMSAddress{}, err
	}
	return AMSAddress{NetID: id, Port: port}, nil
}

// NetIDString returns the AMS NetID in dotted notation
// (e.g. "192.168.1.1.1.1"). Use String for the full "NetID:Port" form.
func (a AMSAddress) NetIDString() string {
	return fmt.Sprintf("%d.%d.%d.%d.%d.%d", a.NetID[0], a.NetID[1], a.NetID[2], a.NetID[3], a.NetID[4], a.NetID[5])
}

// String returns "NetID:Port" in dotted notation, e.g. "1.2.3.4.1.1:851".
func (a AMSAddress) String() string {
	return fmt.Sprintf("%s:%d", a.NetIDString(), a.Port)
}

// Equal reports whether a and other have the same NetID and Port.
func (a AMSAddress) Equal(other AMSAddress) bool {
	return a.NetID == other.NetID && a.Port == other.Port
}

// encode lives on *Client. Session callers reach it via s.client.encode
// at the rare sites still on Session.
func (c *Client) encode(command CommandID, data []byte, invokeID uint32) ([]byte, error) {
	// Snapshot source under lock to avoid race with Session writing c.source
	// during reconnect's auto-derive path. target is set at construction
	// and never mutated after that.
	c.tx.connMu.Lock()
	source := c.source
	c.tx.connMu.Unlock()
	c.logger.Log(context.Background(), LevelTrace, "Starting encoding of AMS header",
		"command", command,
		"target", c.target,
		"source", source,
		"ID", invokeID,
		"length of data", len(data))
	tcpHeader := &amsTCPHeader{
		Unknown1: 0,
		System:   0,
		Length:   uint32(AMSHeaderSize + len(data)),
	}
	header := &AMSHeader{
		Target:    c.target,
		Source:    source,
		Command:   command,
		State:     uint16(4),
		Length:    uint32(len(data)),
		ErrorCode: uint32(0),
		InvokeID:  invokeID,
	}

	buff := new(bytes.Buffer)
	if err := binary.Write(buff, binary.LittleEndian, tcpHeader); err != nil {
		return nil, err
	}
	if err := binary.Write(buff, binary.LittleEndian, header); err != nil {
		return nil, err
	}
	if err := binary.Write(buff, binary.LittleEndian, data); err != nil {
		c.logger.Error("binary.Write failed", "error", err)
		return nil, err
	}
	c.logger.Log(context.Background(), LevelTrace, "data to transmit", "data", data)
	c.logger.Log(context.Background(), LevelTrace, "The encoded AMS header:", hexAttr("bytes", buff.Bytes()))
	return buff.Bytes(), nil
}
