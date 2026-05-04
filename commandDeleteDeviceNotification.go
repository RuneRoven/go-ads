package ads

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

// DeleteDeviceNotification deletes a device notification by handle.
func (conn *Connection) DeleteDeviceNotification(handle uint32) error {
	request := &bytes.Buffer{}
	type deleteNotificationCommandPacket struct {
		Handle uint32
	}
	content := deleteNotificationCommandPacket{
		handle,
	}
	if err := binary.Write(request, binary.LittleEndian, content); err != nil {
		return fmt.Errorf("binary.Write failed: %w", err)
	}
	// Try to send the request
	resp, err := conn.sendRequest(CommandIDDeleteDeviceNotification, request.Bytes())
	if err != nil {
		conn.logger.Warn("error deleting handle", "handle", handle, "error", err)
		return err
	}

	// Check the result error code
	respBuffer := bytes.NewBuffer(resp)
	var adsError ReturnCode
	if err = binary.Read(respBuffer, binary.LittleEndian, &adsError); err != nil {
		return fmt.Errorf("failed to parse DeleteDeviceNotification response: %w", err)
	}
	if adsError > 0 {
		conn.logger.Warn("error deleting handle", "handle", handle, "errorCode", uint32(adsError))
		return fmt.Errorf("ADS error in DeleteDeviceNotification: %w", adsError)
	}
	conn.symbolLock.Lock()
	if sym := conn.activeNotifications[handle]; sym != nil {
		conn.removeNotificationConfig(sym.FullName)
	}
	delete(conn.activeNotifications, handle)
	if len(conn.activeNotifications) == 0 {
		conn.notificationChannel = nil
	}
	conn.symbolLock.Unlock()
	conn.logger.Info("deleted handle", "handle", handle)
	return nil
}
