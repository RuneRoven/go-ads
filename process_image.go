package ads

import (
	"encoding/binary"
	"fmt"
)

// Process Image I/O
//
// EXPERIMENTAL — These methods provide direct access to the PLC's process image
// memory. Unlike symbol-based access (ReadFromSymbol/WriteToSymbol), process image
// operations bypass the symbol table and write raw bytes to I/O memory regions.
//
// WARNING: Writing to the wrong offset can cause unexpected physical output changes
// (motors, valves, actuators). The PLC runtime may overwrite your changes on the
// next scan cycle, or your writes may conflict with the running PLC program.
//
// Use cases:
//   - Diagnostics: reading raw I/O state without symbol resolution
//   - Commissioning: toggling outputs before PLC program is deployed
//   - Testing: verifying I/O wiring by reading/writing specific bits
//
// For normal operation, prefer symbol-based access (ReadFromSymbol/WriteToSymbol)
// which is safer and self-documenting.

// ReadProcessInput reads bytes from the input process image at the given byte offset.
func (conn *Session) ReadProcessInput(byteOffset, length uint32) ([]byte, error) {
	return conn.Read(uint32(GroupIoImageRwib), byteOffset, length)
}

// ReadProcessOutput reads bytes from the output process image at the given byte offset.
func (conn *Session) ReadProcessOutput(byteOffset, length uint32) ([]byte, error) {
	return conn.Read(uint32(GroupIoImageRwob), byteOffset, length)
}

// WriteProcessOutput writes bytes to the output process image at the given byte offset.
func (conn *Session) WriteProcessOutput(byteOffset uint32, data []byte) error {
	return conn.Write(uint32(GroupIoImageRwob), byteOffset, data)
}

// ReadProcessInputBit reads a single bit from the input process image.
// bitIndex must be 0-7 (bit within a single byte).
func (conn *Session) ReadProcessInputBit(byteOffset uint32, bitIndex uint8) (bool, error) {
	if bitIndex > 7 {
		return false, fmt.Errorf("bitIndex must be 0-7, got %d", bitIndex)
	}
	data, err := conn.Read(uint32(GroupIoImageRwix), byteOffset*8+uint32(bitIndex), 1)
	if err != nil {
		return false, err
	}
	if len(data) < 1 {
		return false, fmt.Errorf("ReadProcessInputBit: response too short (%d bytes)", len(data))
	}
	return data[0] != 0, nil
}

// WriteProcessOutputBit writes a single bit to the output process image.
// bitIndex must be 0-7 (bit within a single byte).
func (conn *Session) WriteProcessOutputBit(byteOffset uint32, bitIndex uint8, value bool) error {
	if bitIndex > 7 {
		return fmt.Errorf("bitIndex must be 0-7, got %d", bitIndex)
	}
	v := byte(0)
	if value {
		v = 1
	}
	return conn.Write(uint32(GroupIoImageRwox), byteOffset*8+uint32(bitIndex), []byte{v})
}

// ReadProcessInputSize returns the size of the input process image in bytes.
func (conn *Session) ReadProcessInputSize() (uint32, error) {
	data, err := conn.Read(uint32(GroupIoImageRisize), 0, 4)
	if err != nil {
		return 0, err
	}
	if len(data) < 4 {
		return 0, fmt.Errorf("ReadProcessInputSize: response too short (%d bytes)", len(data))
	}
	return binary.LittleEndian.Uint32(data), nil
}

// ClearProcessInputs writes all input process image bytes to zero.
func (conn *Session) ClearProcessInputs() error {
	return conn.Write(uint32(GroupIoImageCleari), 0, []byte{0})
}

// ClearProcessOutputs writes all output process image bytes to zero.
func (conn *Session) ClearProcessOutputs() error {
	return conn.Write(uint32(GroupIoImageClearo), 0, []byte{0})
}
