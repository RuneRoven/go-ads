package ads

import (
	"sync/atomic"
)

// capabilities consolidates feature-detection state for a single connection.
// Each field tracks whether a particular ADS sub-protocol is supported by
// the PLC. State transitions:
//   - sumReadCmd: 0 = unchecked (try Ex2 first); uint32(GroupSumupReadEx2) = use 0xF084;
//     uint32(GroupSumupReadEx) = use 0xF083; 1 = no sum read support (individual reads).
//   - sumWriteState / sumAddNotifState / sumDeleteNotifState: 0 = unchecked,
//     1 = supported, 2 = unsupported. Add (0xF085) and Delete (0xF086) are
//     tracked separately because a PLC may support one and not the other.
//   - chunkedDownloadSupported / chunkedDownloadChecked: pair tracks whether a chunked
//     download probe has been done and whether it succeeded.
//
// Reset is implicit: see the comment at the bottom of this file — a fresh
// Client (allocated on every Connect / dialAndStart) starts zeroed, so no
// explicit reset method is needed.
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
