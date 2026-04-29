package ads

import "encoding/binary"

// ReadProcessInput reads bytes from the input process image at the given byte offset.
func (conn *Connection) ReadProcessInput(byteOffset, length uint32) ([]byte, error) {
	return conn.Read(uint32(GroupIoImageRwib), byteOffset, length)
}

// ReadProcessOutput reads bytes from the output process image at the given byte offset.
func (conn *Connection) ReadProcessOutput(byteOffset, length uint32) ([]byte, error) {
	return conn.Read(uint32(GroupIoImageRwob), byteOffset, length)
}

// WriteProcessOutput writes bytes to the output process image at the given byte offset.
func (conn *Connection) WriteProcessOutput(byteOffset uint32, data []byte) error {
	return conn.Write(uint32(GroupIoImageRwob), byteOffset, data)
}

// ReadProcessInputBit reads a single bit from the input process image.
func (conn *Connection) ReadProcessInputBit(byteOffset uint32, bitIndex uint8) (bool, error) {
	data, err := conn.Read(uint32(GroupIoImageRwix), byteOffset*8+uint32(bitIndex), 1)
	if err != nil {
		return false, err
	}
	return data[0] != 0, nil
}

// WriteProcessOutputBit writes a single bit to the output process image.
func (conn *Connection) WriteProcessOutputBit(byteOffset uint32, bitIndex uint8, value bool) error {
	v := byte(0)
	if value {
		v = 1
	}
	return conn.Write(uint32(GroupIoImageRwox), byteOffset*8+uint32(bitIndex), []byte{v})
}

// ReadProcessInputSize returns the size of the input process image in bytes.
func (conn *Connection) ReadProcessInputSize() (uint32, error) {
	data, err := conn.Read(uint32(GroupIoImageRisize), 0, 4)
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint32(data), nil
}

// ClearProcessInputs writes all input process image bytes to zero.
func (conn *Connection) ClearProcessInputs() error {
	return conn.Write(uint32(GroupIoImageCleari), 0, []byte{0})
}

// ClearProcessOutputs writes all output process image bytes to zero.
func (conn *Connection) ClearProcessOutputs() error {
	return conn.Write(uint32(GroupIoImageClearo), 0, []byte{0})
}
