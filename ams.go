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

// stringToNetID converts a dotted notation NetID string (e.g. "192.168.1.1.1.1") to a 6-byte array.
// Returns an error if the string is malformed (wrong number of parts or non-numeric values).
func stringToNetID(source string) (result [6]byte, err error) {
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

func (conn *Session) encode(command CommandID, data []byte, invokeID uint32) ([]byte, error) {
	// Snapshot source under lock to avoid race with Reconnect writing conn.source.
	// target is write-once (set in NewConnection), so no lock needed.
	conn.tx.connMu.Lock()
	source := conn.source
	conn.tx.connMu.Unlock()
	conn.logger.Log(context.Background(), LevelTrace, "Starting encoding of AMS header",
		"command", command,
		"target", conn.target,
		"source", source,
		"ID", invokeID,
		"length of data", len(data))
	tcpHeader := &amsTCPHeader{
		Unknown1: 0,
		System:   0,
		Length:   uint32(32 + len(data)),
	}
	header := &amsHeader{
		Target:    conn.target,
		Source:    source,
		Command:   command,
		State:     uint16(4),
		Length:    uint32(len(data)),
		ErrorCode: uint32(0),
		InvokeID:  invokeID,
	}

	buff := new(bytes.Buffer)
	err := binary.Write(buff, binary.LittleEndian, tcpHeader)
	if err != nil {
		return nil, err
	}
	err = binary.Write(buff, binary.LittleEndian, header)
	if err != nil {
		return nil, err
	}
	err = binary.Write(buff, binary.LittleEndian, data)
	conn.logger.Log(context.Background(), LevelTrace, "data to transmit", "data", data)
	if err != nil {
		conn.logger.Error("binary.Write failed", "error", err)
		return nil, err
	}

	conn.logger.Log(context.Background(), LevelTrace, "The encoded AMS header:", hexAttr("bytes", buff.Bytes()))

	return buff.Bytes(), nil
}
