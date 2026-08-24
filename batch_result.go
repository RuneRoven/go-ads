package ads

import (
	"errors"
	"fmt"
	"sort"
	"strings"
)

// Reasons the library itself, rather than the PLC, produced no value for an
// item of a batch read or write. They appear in BatchItemError.Skipped, wrapped
// around the underlying cause where there is one, so both errors.Is(err,
// ErrBatchSymbolUnresolved) and errors.Is(err, theUnderlyingCause) hold.
var (
	// ErrBatchSymbolUnresolved: the symbol could not be resolved to an address,
	// so the item was never put on the wire.
	ErrBatchSymbolUnresolved = errors.New("symbol could not be resolved for batch")

	// ErrBatchValueUnserializable: the value could not be encoded for this
	// symbol's data type, so the write was never put on the wire. A type change
	// under an online change produces this.
	ErrBatchValueUnserializable = errors.New("value could not be serialized for batch write")

	// ErrBatchValueUnparsable: the PLC answered but the bytes did not decode
	// against the cached type — the shape an undetected online change takes.
	ErrBatchValueUnparsable = errors.New("batch value could not be parsed")

	// ErrBatchSymbolVanished: the cache entry disappeared mid-roundtrip
	// (loadSymbols swapped the cache), so the PLC's answer has nowhere to land.
	ErrBatchSymbolVanished = errors.New("symbol removed from cache during batch")

	// ErrBatchNoResult: the PLC's response carried fewer items than the request,
	// so this item has no verdict at all.
	ErrBatchNoResult = errors.New("no result returned for symbol")
)

// BatchItemError is the per-item failure detail of a batch symbol read or
// write. It carries the same three-state contract as SumNotificationResult, so
// a caller can tell a device verdict from a library-side drop:
//
//   - Skipped != nil: the library, not the PLC, is why this item has no value —
//     it was never sent (unresolved symbol, unserializable value, short
//     response) or its answer could not be used (cache swapped, parse failed).
//     Error is not meaningful.
//   - Skipped == nil: the PLC gave a verdict on this item and Error carries its
//     return code. A genuinely absent symbol (ReturnCodeDeviceSymbolNoFound,
//     0x0710) lands here, as does a stale handle after a runtime restart
//     (0x0710/0x0711).
//
// Only failures are reported; an item that succeeded never appears.
type BatchItemError struct {
	// Symbol is the symbol name as the caller passed it.
	Symbol string
	// Error is the PLC's per-item return code. Meaningful only when Skipped is nil.
	Error ReturnCode
	// Skipped is non-nil when the library produced no value for this item.
	Skipped error
}

// String renders one item as "MAIN.a (code 0x710)" or "MAIN.a (skipped: ...)".
func (i BatchItemError) String() string {
	if i.Skipped != nil {
		return fmt.Sprintf("%s (skipped: %v)", i.Symbol, i.Skipped)
	}
	return fmt.Sprintf("%s (code 0x%X)", i.Symbol, uint32(i.Error))
}

// BatchError reports the items of a batch symbol read or write that produced no
// value, while the successful items are still returned in the call's map. It is
// returned only when at least one item failed, and it is the only error these
// calls return that leaves the map usable: a bare error (including a wrapped
// AMSError) means the transport failed and no item's outcome is known.
//
// Recover it with errors.As:
//
//	values, err := sess.ReadMultipleSymbols(ctx, names)
//	var batchErr *ads.BatchError
//	if errors.As(err, &batchErr) {
//		// values holds batchErr.Succeeded entries and is safe to use;
//		// batchErr.Items names the ones that failed and why.
//	} else if err != nil {
//		// transport failure — values is not trustworthy
//	}
//
// Batch size does not change the contract: one absent symbol read on its own
// and the same symbol read as one of forty both come back as a BatchError
// naming that one symbol, with every other value present in the map.
type BatchError struct {
	// Op is "read" or "write".
	Op string
	// Requested is how many symbols the caller asked for.
	Requested int
	// Succeeded is how many produced a valid result.
	Succeeded int
	// Items holds one entry per failed symbol, never empty.
	Items []BatchItemError
}

// maxNamedBatchItems caps how many symbols an Error() string names, so a
// 500-symbol batch that failed wholesale does not produce a 20KB log line. The
// full set is always in Items.
const maxNamedBatchItems = 8

// Error names the failed symbols and their reasons.
func (e *BatchError) Error() string {
	var b strings.Builder
	fmt.Fprintf(&b, "batch %s: %d of %d symbols failed: ", e.Op, len(e.Items), e.Requested)
	for i, item := range e.Items {
		if i == maxNamedBatchItems {
			fmt.Fprintf(&b, " and %d more", len(e.Items)-i)
			break
		}
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(item.String())
	}
	return b.String()
}

// Unwrap exposes the library-side skip reasons so errors.Is can match the
// ErrBatch* sentinels through the BatchError. Per-item PLC codes are not
// unwrapped: a ReturnCode reached through errors.As would be indistinguishable
// from a call-level failure, which is the ambiguity this type exists to remove.
func (e *BatchError) Unwrap() []error {
	var errs []error
	for _, item := range e.Items {
		if item.Skipped != nil {
			errs = append(errs, item.Skipped)
		}
	}
	return errs
}

// sortBatchItems orders items by symbol name and returns them. The write path
// iterates the caller's map, so without this the error message — and any log
// line built from it — differs run to run for the same failure.
func sortBatchItems(items []BatchItemError) []BatchItemError {
	sort.Slice(items, func(i, j int) bool { return items[i].Symbol < items[j].Symbol })
	return items
}

// newBatchError builds the error for a batch that had at least one failure.
// Returns nil when items is empty, so call sites can return it unconditionally.
func newBatchError(op string, requested, succeeded int, items []BatchItemError) error {
	if len(items) == 0 {
		return nil
	}
	return &BatchError{Op: op, Requested: requested, Succeeded: succeeded, Items: items}
}
