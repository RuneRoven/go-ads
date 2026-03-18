package ads

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/rs/zerolog/log"
)

// DeleteDeviceNotification deletes a device notification by handle.
func (conn *Connection) DeleteDeviceNotification(handle uint32) error {
	request := &bytes.Buffer{}
	type deleteNotificationCommandPacket struct {
		Handle uint32
	}
	var content = deleteNotificationCommandPacket{
		handle,
	}
	if err := binary.Write(request, binary.LittleEndian, content); err != nil {
		return fmt.Errorf("binary.Write failed: %w", err)
	}
	// Try to send the request
	resp, err := conn.sendRequest(CommandIDDeleteDeviceNotification, request.Bytes())
	if err != nil {
		log.Warn().
			Uint32("handle", handle).
			Err(err).
			Msg("error deleting handle")
		return err
	}

	// Check the result error code
	respBuffer := bytes.NewBuffer(resp)
	var adsError ReturnCode
	if err = binary.Read(respBuffer, binary.LittleEndian, &adsError); err != nil {
		return fmt.Errorf("failed to parse DeleteDeviceNotification response: %w", err)
	}
	if adsError > 0 {
		log.Warn().
			Uint32("handle", handle).
			Uint32("errorCode", uint32(adsError)).
			Msg("error deleting handle")
		return fmt.Errorf("ADS error in DeleteDeviceNotification: %w", adsError)
	}
	conn.symbolLock.Lock()
	delete(conn.activeNotifications, handle)
	conn.symbolLock.Unlock()
	log.Info().
		Uint32("handle", handle).
		Msg("deleted handle")
	return nil
}
