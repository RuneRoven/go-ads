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

type amsHeader struct {
	Target    AMSAddress
	Source    AMSAddress
	Command   CommandID
	State     uint16
	Length    uint32
	ErrorCode uint32
	InvokeID  uint32
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
		Length:   uint32(32 + len(data)),
	}
	header := &amsHeader{
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
