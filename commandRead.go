package ads

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
)

func (conn *Connection) Read(group uint32, offset uint32, length uint32) (data []byte, err error) {
	request := new(bytes.Buffer)
	type readCommandPacket struct {
		Group  uint32
		Offset uint32
		Length uint32
	}
	content := readCommandPacket{
		group,
		offset,
		length,
	}

	err = binary.Write(request, binary.LittleEndian, content)
	if err != nil {
		conn.logger.Error("binary.Write failed", "error", err)
		return nil, err
	}

	conn.logger.Log(context.Background(), LevelTrace, "request", "request", content)

	// Try to send the request
	resp, err := conn.sendRequest(CommandIDRead, request.Bytes())
	if err != nil {
		conn.logger.Error("send request failed", "error", err)
		return
	}

	// Check the result error code
	type readResponse struct {
		Error  ReturnCode
		Length uint32
	}
	respBuff := bytes.NewBuffer(resp)
	response := &readResponse{}
	err = binary.Read(respBuff, binary.LittleEndian, response)
	if err != nil {
		return nil, fmt.Errorf("failed to parse Read response: %w", err)
	}
	if response.Error > 0 {
		err = fmt.Errorf("ADS error in Read: %w", response.Error)
		return
	}
	// F-19: validate that the body has the bytes the response header declared.
	// Returning the raw remaining buffer would silently pass through truncated
	// or padded payloads. Trust the declared Length; reject undersized.
	if uint64(respBuff.Len()) < uint64(response.Length) {
		return nil, fmt.Errorf("Read: declared length %d, body has %d bytes", response.Length, respBuff.Len())
	}
	return respBuff.Next(int(response.Length)), nil
}
