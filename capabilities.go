package ads

import (
	"sync/atomic"
)

// capabilities holds per-connection feature detection. sumReadCmd is 0 unchecked,
// the group constant for the variant in use, or 1 for no sum read at all; the
// sum*State fields are 0 unchecked / 1 supported / 2 unsupported, tracked
// separately because a PLC may serve Add but not Delete. Reset is implicit -- a
// fresh Client is allocated on every dial and starts zeroed.
type capabilities struct {
	sumReadCmd               atomic.Uint32
	sumWriteState            atomic.Uint32
	sumAddNotifState         atomic.Uint32
	sumDeleteNotifState      atomic.Uint32
	chunkedDownloadSupported atomic.Bool
	chunkedDownloadChecked   atomic.Bool
}

func (c *capabilities) SumReadCmdLoad() uint32    { return c.sumReadCmd.Load() }
func (c *capabilities) SumReadCmdStore(v uint32)  { c.sumReadCmd.Store(v) }
func (c *capabilities) SumWriteStateLoad() uint32 { return c.sumWriteState.Load() }
func (c *capabilities) SumWriteStateStore(v uint32) {
	c.sumWriteState.Store(v)
}

func (c *capabilities) SumWriteStateCAS(old, new uint32) bool {
	return c.sumWriteState.CompareAndSwap(old, new)
}
func (c *capabilities) SumAddNotifStateLoad() uint32 { return c.sumAddNotifState.Load() }
func (c *capabilities) SumAddNotifStateStore(v uint32) {
	c.sumAddNotifState.Store(v)
}

func (c *capabilities) SumAddNotifStateCAS(old, new uint32) bool {
	return c.sumAddNotifState.CompareAndSwap(old, new)
}
func (c *capabilities) SumDeleteNotifStateLoad() uint32 { return c.sumDeleteNotifState.Load() }
func (c *capabilities) SumDeleteNotifStateStore(v uint32) {
	c.sumDeleteNotifState.Store(v)
}

func (c *capabilities) SumDeleteNotifStateCAS(old, new uint32) bool {
	return c.sumDeleteNotifState.CompareAndSwap(old, new)
}

func (c *capabilities) ChunkedDownloadCheckedLoad() bool   { return c.chunkedDownloadChecked.Load() }
func (c *capabilities) ChunkedDownloadCheckedStore(v bool) { c.chunkedDownloadChecked.Store(v) }
func (c *capabilities) ChunkedDownloadSupportedLoad() bool {
	return c.chunkedDownloadSupported.Load()
}

func (c *capabilities) ChunkedDownloadSupportedStore(v bool) {
	c.chunkedDownloadSupported.Store(v)
}

// Reset is implicit: capabilities lives on *Client and a fresh Client is
// allocated on every Connect / dialAndStart, so the per-attempt struct
// value starts zeroed.
