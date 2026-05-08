package ads

import (
	"sync"

	"go.uber.org/atomic"
)

// symbolCache owns the connection-level symbol metadata: the symbol map,
// the data-type table, the PLC's reported symbol version (for change
// detection), and discovery-mode flags that track which load function was
// used (LoadSymbols, LoadSymbolsSlow, LoadSymbolList, LoadDataTypes).
//
// Lock also covers Symbol mutation during parse() — Symbol objects live
// in the cache.symbols map and parse() rewrites their Value/Valid
// fields. Lock ordering: NEVER hold both cache.lock and notifs.lock at
// the same time. Paths that need both must release one before acquiring
// the other.
//
// generation increments under lock on every cache.symbols SWAP (loadSymbols,
// LoadSymbolList, LoadDataTypes, on-demand reset in reloadSymbols). It does
// NOT increment on simple insert (e.g. on-demand getSymbol) - existing
// pointers stay valid across an insert, so insert is not a stranding event.
//
// Callers that need to publish a *Symbol pointer they obtained pre-roundtrip
// into another data structure (e.g. notifs.activeNotifications) MUST capture
// the generation before resolve and recheck before commit; if the value
// changed, the pointer is stranded and must be discarded. Closes the
// residual race window between cache.lock release and notifs.lock acquire
// that the simple re-fetch pattern leaves open.
type symbolCache struct {
	lock               sync.Mutex
	generation         atomic.Uint64
	symbols            map[string]*Symbol
	datatypes          map[string]SymbolUploadDataType
	symbolVersion      uint8
	onDemandSymbols    map[string]bool
	symbolListLoaded   bool
	symbolsFullyLoaded bool
	datatypesLoaded    bool
}
