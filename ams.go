package ads

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"strconv"
	"strings"

	"github.com/rs/zerolog/log"
)

type amsTCPHeader struct {
	Unknown1 uint8
	System   uint8
	Length   uint32
}

type amsHeader struct {
	Target    AmsAddress
	Source    AmsAddress
	Command   CommandID
	State     uint16
	Length    uint32
	ErrorCode uint32
	InvokeID  uint32
}

// StringToNetID converts a dotted notation NetID string (e.g. "192.168.1.1.1.1") to a 6-byte array.
// Returns an error if the string is malformed (wrong number of parts or non-numeric values).
func StringToNetID(source string) ([6]byte, error) {
	return stringToNetID(source)
}

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

func (conn *Connection) encode(command CommandID, data []byte, invokeID uint32) ([]byte, error) {
	log.Trace().
		Interface("command", command).
		Interface("target", conn.target).
		Interface("source", conn.source).
		Uint32("ID", invokeID).
		Int("length of data", len(data)).
		Msg("Starting encoding of AMS header")
	tcpHeader := &amsTCPHeader{
		Unknown1: 0,
		System:   0,
		Length:   uint32(32 + len(data)),
	}
	header := &amsHeader{
		Target:    conn.target,
		Source:    conn.source,
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
	log.Trace().
		Bytes("data", data).
		Msg("data to transmit")
	if err != nil {
		log.Error().
			Err(err).
			Msg("binary.Write failed")
		return nil, err
	}

	log.Trace().
		Hex("bytes", buff.Bytes()).
		Msg("The encoded AMS header:")

	return buff.Bytes(), nil
}
