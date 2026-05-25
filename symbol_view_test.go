package ads

import (
	"testing"
	"time"
)

// symbol_view_test.go — SymbolView snapshot + iteration unit tests.
//
// Covers:
//   - R-VIEW-001 (snapshot consistency)
//   - R-VIEW-002 (IsValid distinguishes zero from live)
//   - R-VIEW-003 (Children returns fresh map)
//   - R-VIEW-004 (ChildrenWalk no deadlock — fn may take cache.lock)
//   - R-VIEW-005 (ListSymbols error before LoadSymbols)
//   - R-VIEW-007 (SymbolView field-read after Close)

// newViewTestSession constructs a Session minimal enough for SymbolView
// tests: cache + lifecycle. No client, no transport.
func newViewTestSession() *Session {
	return &Session{
		cache:         &symbolCache{symbols: map[string]*Symbol{}, onDemandSymbols: map[string]bool{}},
		notifications: &notificationManager{activeNotifications: make(map[uint32]*Symbol), configsByKey: make(map[string]struct{})},
		lifecycle:     &sessionLifecycle{closedCh: make(chan struct{})},
		logger:        getDefaultLogger(),
	}
}

// captureSymbol seeds the cache with a Symbol and returns a SymbolView.
func captureSymbol(sess *Session, name, value string, parsed bool) (SymbolView, *Symbol) {
	sym := &Symbol{
		FullName:    name,
		Name:        name,
		DataType:    "INT",
		Length:      2,
		Value:       value,
		Valid:       parsed,
		ValueParsed: parsed,
		Handle:      0xFEEDFACE,
	}
	sess.cache.lock.Lock()
	sess.cache.symbols[symbolKey(name)] = sym
	sess.cache.lock.Unlock()
	v, _ := sess.GetSymbol(name) // takes cache.lock internally for view()
	return v, sym
}

// TestSymbolView_SnapshotConsistency captures a view, mutates the underlying
// Symbol via cache.lock, asserts the view's fields reflect the SNAPSHOT
// instant (not the post-mutation state).
//
// Validates: R-VIEW-001 (SymbolView is a snapshot).
func TestSymbolView_SnapshotConsistency(t *testing.T) {
	sess := newViewTestSession()
	view, sym := captureSymbol(sess, "MAIN.x", "42", true)

	// Mutate Symbol fields under cache.lock as parse() would.
	sess.cache.lock.Lock()
	sym.Value = "999"
	sym.Valid = false
	sym.ValueParsed = false
	sym.Handle = 0xDEAD
	sess.cache.lock.Unlock()

	// View must show pre-mutation values (it's a snapshot).
	if view.Value != "42" {
		t.Errorf("view.Value = %q, want %q (snapshot)", view.Value, "42")
	}
	if !view.Parsed {
		t.Errorf("view.Parsed = false, want true (snapshot)")
	}
	if view.Handle != 0xFEEDFACE {
		t.Errorf("view.Handle = 0x%X, want 0xFEEDFACE (snapshot)", view.Handle)
	}
}

// TestSymbolView_IsValid checks both zero-value and live cases.
//
// Validates: R-VIEW-002 (IsValid distinguishes zero-value from live).
func TestSymbolView_IsValid(t *testing.T) {
	var zero SymbolView
	if zero.IsValid() {
		t.Error("zero-value SymbolView.IsValid() = true, want false")
	}

	sess := newViewTestSession()
	view, _ := captureSymbol(sess, "MAIN.y", "1", true)
	if !view.IsValid() {
		t.Error("captured SymbolView.IsValid() = false, want true")
	}
}

// TestSymbolView_ChildrenReturnsFreshMap captures a SymbolView with children,
// calls Children() twice, mutates the first map, asserts the second
// returned map is unaffected.
//
// Validates: R-VIEW-003 (Children returns fresh map per call).
func TestSymbolView_ChildrenReturnsFreshMap(t *testing.T) {
	sess := newViewTestSession()

	child := &Symbol{
		FullName: "MAIN.s.f",
		Name:     "f",
		DataType: "INT",
		Length:   2,
		Handle:   0xC0DE0001,
	}
	parent := &Symbol{
		FullName: "MAIN.s",
		Name:     "s",
		DataType: "ST_X",
		Children: map[string]*Symbol{"f": child},
		Handle:   0xC0DE0002,
	}
	child.Parent = parent
	sess.cache.lock.Lock()
	sess.cache.symbols[symbolKey(parent.FullName)] = parent
	sess.cache.symbols[symbolKey(child.FullName)] = child
	sess.cache.lock.Unlock()

	pv, err := sess.GetSymbol(parent.FullName)
	if err != nil {
		t.Fatalf("GetSymbol: %v", err)
	}

	first := pv.Children()
	if len(first) == 0 {
		t.Fatal("Children() returned empty map; want one child")
	}
	// Mutate the first map.
	delete(first, "f")
	first["BOGUS"] = SymbolView{}

	second := pv.Children()
	if len(second) != 1 {
		t.Errorf("second Children() len = %d, want 1 (fresh allocation)", len(second))
	}
	if _, ok := second["f"]; !ok {
		t.Errorf("second Children() missing original key 'f'")
	}
	if _, ok := second["BOGUS"]; ok {
		t.Errorf("second Children() leaked mutation from first call")
	}
}

// TestSymbolView_ChildrenWalk_FnMayTakeCacheLock asserts ChildrenWalk
// snapshots the subtree, releases cache.lock, then invokes fn. fn may
// safely call any Session method (e.g. GetSymbol which itself takes
// cache.lock) without deadlock.
//
// Validates: R-VIEW-004 (ChildrenWalk collect-then-iterate, no deadlock).
func TestSymbolView_ChildrenWalk_FnMayTakeCacheLock(t *testing.T) {
	sess := newViewTestSession()

	leaf := &Symbol{FullName: "MAIN.s.f", Name: "f", DataType: "INT", Length: 2, Handle: 0xBABE0001}
	parent := &Symbol{
		FullName: "MAIN.s", Name: "s", DataType: "ST_X",
		Children: map[string]*Symbol{"f": leaf}, Handle: 0xBABE0002,
	}
	leaf.Parent = parent
	sess.cache.lock.Lock()
	sess.cache.symbols[symbolKey(parent.FullName)] = parent
	sess.cache.symbols[symbolKey(leaf.FullName)] = leaf
	sess.cache.lock.Unlock()

	pv, err := sess.GetSymbol(parent.FullName)
	if err != nil {
		t.Fatalf("GetSymbol: %v", err)
	}

	// Run with a deadline-bound goroutine; deadlock would hang.
	done := make(chan struct{})
	go func() {
		defer close(done)
		pv.ChildrenWalk(func(v SymbolView) bool {
			// fn calls GetSymbol, which takes cache.lock. A retained
			// cache.lock around fn would deadlock here.
			_, _ = sess.GetSymbol(v.FullName)
			return true
		})
	}()
	select {
	case <-done:
		// success — no deadlock.
	case <-time.After(2 * time.Second):
		t.Fatal("ChildrenWalk fn deadlocked (cache.lock held during iteration)")
	}
}

// TestListSymbols_ErrorBeforeLoadSymbols asserts ListSymbols returns an
// error when full discovery has not been performed (cache.symbolsFullyLoaded
// is false).
//
// Validates: R-VIEW-005 (ListSymbols requires full discovery).
func TestListSymbols_ErrorBeforeLoadSymbols(t *testing.T) {
	sess := newViewTestSession()
	// cache.symbolsFullyLoaded is false; ListSymbols must fail.
	_, err := sess.ListSymbols()
	if err == nil {
		t.Error("ListSymbols on un-discovered cache: err = nil, want error")
	}
}

// TestSymbolView_FieldReadAfterClose asserts reading view fields (Name,
// FullName, Value, Parsed) after the Session reaches the Closed state
// returns the snapshotted values without panic. Children()/ChildrenWalk()
// may legally fail or return nil after Close — this test asserts only the
// chosen behavior.
//
// Uses a synthetic Session (no real Client) and drives the FSM directly
// to Closed; full Close() with handle-release walks would dereference the
// nil client.
//
// Validates: R-VIEW-007 (read-after-Close is allowed, snapshot semantics).
func TestSymbolView_FieldReadAfterClose(t *testing.T) {
	sess := newViewTestSession()
	view, _ := captureSymbol(sess, "MAIN.z", "7", true)

	// Drive FSM to Closed without invoking real Close() (which would
	// nil-deref c.client). The contract under test is read-after-Closed,
	// not the close path itself.
	sess.lifecycle.state.transitionTo(SessionStateConstructed)
	sess.lifecycle.state.transitionTo(SessionStateClosed)
	close(sess.lifecycle.closedCh)

	// Field reads must not panic; values are the snapshot.
	if view.Name != "MAIN.z" {
		t.Errorf("post-Close view.Name = %q, want %q", view.Name, "MAIN.z")
	}
	if view.FullName != "MAIN.z" {
		t.Errorf("post-Close view.FullName = %q", view.FullName)
	}
	if view.Value != "7" {
		t.Errorf("post-Close view.Value = %q, want %q", view.Value, "7")
	}
	if !view.Parsed {
		t.Errorf("post-Close view.Parsed = false, want true")
	}
	// Children may return nil or a value; assert no panic.
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("Children() post-Close panicked: %v", r)
		}
	}()
	_ = view.Children()
}

// TestCollectSubtreeDepthCap verifies the recursion cap on Symbol.Children
// walks defends against tree corruption (cycle in Children map).
// Validates: R-VIEW-004.
func TestCollectSubtreeDepthCap(t *testing.T) {
	// Build a Symbol cycle: A -> B -> A. addOffset cannot produce this in
	// real cache data, but defensively we should not stack-overflow.
	a := &Symbol{Name: "A", FullName: "A"}
	b := &Symbol{Name: "B", FullName: "A.B"}
	a.Children = map[string]*Symbol{"B": b}
	b.Children = map[string]*Symbol{"A": a}

	conn := newTestConnection()
	defer conn.lifecycle.shutdown()

	// Walk via collectSubtree (lock not actually needed for this test
	// because we are not touching cache.symbols).
	var collected []SymbolView
	collectSubtree(a, conn, &collected)
	// Should NOT stack-overflow; cap=256 limits the walk.
	if len(collected) == 0 {
		t.Fatal("expected at least one collected view before cap fires")
	}
}
