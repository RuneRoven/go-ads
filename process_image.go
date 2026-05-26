package ads

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"
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

// EXPERIMENTAL: ReadProcessInput reads bytes from the input process image at the given byte offset.
func (c *Client) ReadProcessInput(ctx context.Context, byteOffset, length uint32) ([]byte, error) {
	return c.Read(ctx, uint32(GroupIoImageRwib), byteOffset, length)
}

// EXPERIMENTAL: ReadProcessOutput reads bytes from the output process image at the given byte offset.
func (c *Client) ReadProcessOutput(ctx context.Context, byteOffset, length uint32) ([]byte, error) {
	return c.Read(ctx, uint32(GroupIoImageRwob), byteOffset, length)
}

// EXPERIMENTAL: WriteProcessOutput writes bytes to the output process image at the given byte offset.
func (c *Client) WriteProcessOutput(ctx context.Context, byteOffset uint32, data []byte) error {
	return c.Write(ctx, uint32(GroupIoImageRwob), byteOffset, data)
}

// EXPERIMENTAL: ReadProcessInputBit reads a single bit from the input process image.
// bitIndex must be 0-7 (bit within a single byte).
func (c *Client) ReadProcessInputBit(ctx context.Context, byteOffset uint32, bitIndex uint8) (bool, error) {
	if bitIndex > 7 {
		return false, fmt.Errorf("bitIndex must be 0-7, got %d", bitIndex)
	}
	bitOffset := uint64(byteOffset)*8 + uint64(bitIndex)
	if bitOffset > math.MaxUint32 {
		return false, fmt.Errorf("byteOffset %d overflow: bit address %d exceeds uint32 max", byteOffset, bitOffset)
	}
	data, err := c.Read(ctx, uint32(GroupIoImageRwix), uint32(bitOffset), 1)
	if err != nil {
		return false, err
	}
	if len(data) < 1 {
		return false, fmt.Errorf("ReadProcessInputBit: response too short (%d bytes)", len(data))
	}
	return data[0] != 0, nil
}

// EXPERIMENTAL: WriteProcessOutputBit writes a single bit to the output process image.
// bitIndex must be 0-7 (bit within a single byte).
func (c *Client) WriteProcessOutputBit(ctx context.Context, byteOffset uint32, bitIndex uint8, value bool) error {
	if bitIndex > 7 {
		return fmt.Errorf("bitIndex must be 0-7, got %d", bitIndex)
	}
	v := byte(0)
	if value {
		v = 1
	}
	bitOffset := uint64(byteOffset)*8 + uint64(bitIndex)
	if bitOffset > math.MaxUint32 {
		return fmt.Errorf("byteOffset %d overflow: bit address %d exceeds uint32 max", byteOffset, bitOffset)
	}
	return c.Write(ctx, uint32(GroupIoImageRwox), uint32(bitOffset), []byte{v})
}

// EXPERIMENTAL: ReadProcessInputSize returns the size of the input process image in bytes.
func (c *Client) ReadProcessInputSize(ctx context.Context) (uint32, error) {
	data, err := c.Read(ctx, uint32(GroupIoImageRisize), 0, 4)
	if err != nil {
		return 0, err
	}
	if len(data) < 4 {
		return 0, fmt.Errorf("ReadProcessInputSize: response too short (%d bytes)", len(data))
	}
	return binary.LittleEndian.Uint32(data), nil
}

// EXPERIMENTAL: ClearProcessInputs writes all input process image bytes to zero.
func (c *Client) ClearProcessInputs(ctx context.Context) error {
	return c.Write(ctx, uint32(GroupIoImageCleari), 0, []byte{0})
}

// EXPERIMENTAL: ClearProcessOutputs writes all output process image bytes to zero.
func (c *Client) ClearProcessOutputs(ctx context.Context) error {
	return c.Write(ctx, uint32(GroupIoImageClearo), 0, []byte{0})
}
