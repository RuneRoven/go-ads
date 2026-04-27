package ads

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
)

func (conn *Connection) WriteRead(group uint32, offset uint32, readLength uint32, send []byte) (data []byte, err error) {
	request := new(bytes.Buffer)
	type writeReadCommandPacket struct {
		Group       uint32
		Offset      uint32
		ReadLength  uint32
		WriteLength uint32
	}
	content := writeReadCommandPacket{
		group,
		offset,
		readLength,
		uint32(len(send)),
	}

	type readResponse struct {
		Error  ReturnCode
		Length uint32
	}

	err = binary.Write(request, binary.LittleEndian, content)
	if err != nil {
		return nil, fmt.Errorf("binary.Write failed: %w", err)
	}
	err = binary.Write(request, binary.LittleEndian, send)
	if err != nil {
		return nil, fmt.Errorf("binary.Write failed: %w", err)
	}

	conn.logger.Log(context.Background(), LevelTrace, "request", "request", request)

	// Try to send the request
	resp, err := conn.sendRequest(CommandIDReadWrite, request.Bytes())
	if err != nil {
		return
	}

	// Check the result error code
	respBuff := bytes.NewBuffer(resp)
	response := &readResponse{}
	if err = binary.Read(respBuff, binary.LittleEndian, response); err != nil {
		return nil, fmt.Errorf("failed to parse WriteRead response: %w", err)
	}
	if response.Error > 0 {
		return nil, fmt.Errorf("ADS error in WriteRead: %w", response.Error)
	}
	return respBuff.Bytes(), nil
}
