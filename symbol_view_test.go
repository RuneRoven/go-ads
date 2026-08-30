package ads

import (
	"context"
	"log/slog"
	"strings"
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
		cache:         &symbolCache{symbols: map[string]*symbol{}, onDemandSymbols: map[string]bool{}},
		notifications: &notificationManager{activeNotifications: make(map[uint32]activeNotification), configsByKey: make(map[string]struct{}), orphanSeen: make(map[uint32]time.Time), orphanSem: make(chan struct{}, orphanDeleteMaxConcurrency)},
		lifecycle:     &sessionLifecycle{closedCh: make(chan struct{})},
		logger:        getDefaultLogger(),
	}
}

// captureSymbol seeds the cache with a symbol and returns a SymbolView.
func captureSymbol(sess *Session, name, value string, parsed bool) (SymbolView, *symbol) {
	sym := &symbol{
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
	v, _ := sess.GetSymbol(context.Background(), name) // takes cache.lock internally for view()
	return v, sym
}

// TestSymbolView_SnapshotConsistency captures a view, mutates the underlying
// symbol via cache.lock, asserts the view's fields reflect the SNAPSHOT
// instant (not the post-mutation state).
//
// Validates: R-VIEW-001 (SymbolView is a snapshot).
func TestSymbolView_SnapshotConsistency(t *testing.T) {
	sess := newViewTestSession()
	view, sym := captureSymbol(sess, "MAIN.x", "42", true)

	// Mutate symbol fields under cache.lock as parse() would.
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

	child := &symbol{
		FullName: "MAIN.s.f",
		Name:     "f",
		DataType: "INT",
		Length:   2,
		Handle:   0xC0DE0001,
	}
	parent := &symbol{
		FullName: "MAIN.s",
		Name:     "s",
		DataType: "ST_X",
		Children: map[string]*symbol{"f": child},
		Handle:   0xC0DE0002,
	}
	child.Parent = parent
	sess.cache.lock.Lock()
	sess.cache.symbols[symbolKey(parent.FullName)] = parent
	sess.cache.symbols[symbolKey(child.FullName)] = child
	sess.cache.lock.Unlock()

	pv, err := sess.GetSymbol(context.Background(), parent.FullName)
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

	leaf := &symbol{FullName: "MAIN.s.f", Name: "f", DataType: "INT", Length: 2, Handle: 0xBABE0001}
	parent := &symbol{
		FullName: "MAIN.s", Name: "s", DataType: "ST_X",
		Children: map[string]*symbol{"f": leaf}, Handle: 0xBABE0002,
	}
	leaf.Parent = parent
	sess.cache.lock.Lock()
	sess.cache.symbols[symbolKey(parent.FullName)] = parent
	sess.cache.symbols[symbolKey(leaf.FullName)] = leaf
	sess.cache.lock.Unlock()

	pv, err := sess.GetSymbol(context.Background(), parent.FullName)
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
			_, _ = sess.GetSymbol(context.Background(), v.FullName)
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

// TestCollectSubtreeDepthCap verifies the recursion cap on symbol.Children
// walks defends against tree corruption (cycle in Children map).
// Validates: R-VIEW-004.
func TestCollectSubtreeDepthCap(t *testing.T) {
	// Build a symbol cycle: A -> B -> A. addOffset cannot produce this in
	// real cache data, but defensively we should not stack-overflow.
	a := &symbol{Name: "A", FullName: "A"}
	b := &symbol{Name: "B", FullName: "A.B"}
	a.Children = map[string]*symbol{"B": b}
	b.Children = map[string]*symbol{"A": a}

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

// TestBaseTypeName_LayeredResolution exercises the public BaseTypeName
// orchestrator across its three layers in the documented priority order:
//  1. Protocol BaseType (ADST_ code) — authoritative when set.
//  2. Datatype-table lookup by DataType name — for ADST_BIGTYPE aliases.
//  3. Size-based inference — 1- and 2-byte widths only (4/8 deliberately
//     refused to avoid REAL/LREAL ambiguity).
//
// A regression that swaps the layer order would silently corrupt parsing
// for symbols where layers disagree (TC2 enums, type aliases).
func TestBaseTypeName_LayeredResolution(t *testing.T) {
	t.Run("layer1_adst_primitive_wins", func(t *testing.T) {
		// BaseType=ADSTReal32 should resolve to "REAL" regardless of any
		// table entry or size that might disagree.
		sess := newViewTestSession()
		sess.cache.datatypes = map[string]SymbolUploadDataType{
			"FakeAlias": {DataType: "INT"}, // would mis-resolve if layer 2 ran
		}
		view := SymbolView{
			BaseType: ADSTReal32,
			DataType: "FakeAlias",
			Length:   4,
			conn:     sess,
		}
		if got := view.BaseTypeName(); got != "REAL" {
			t.Errorf("BaseTypeName = %q, want REAL (layer 1 must win)", got)
		}
	})

	t.Run("layer2_datatype_table_for_bigtype", func(t *testing.T) {
		// BaseType=ADSTBigType (composite) forces layer 1 to return "";
		// layer 2 looks up "MyAlias" in the table and returns its DataType.
		sess := newViewTestSession()
		sess.cache.datatypes = map[string]SymbolUploadDataType{
			"MyAlias": {DataType: "DINT"},
		}
		view := SymbolView{
			BaseType: ADSTBigType,
			DataType: "MyAlias",
			Length:   4,
			conn:     sess,
		}
		if got := view.BaseTypeName(); got != "DINT" {
			t.Errorf("BaseTypeName = %q, want DINT (layer 2 table lookup)", got)
		}
	})

	t.Run("layer3_size_inference_1byte", func(t *testing.T) {
		// No table loaded; BaseType=BigType. Size=1 → SINT.
		sess := newViewTestSession()
		view := SymbolView{
			BaseType: ADSTBigType,
			DataType: "UnknownType",
			Length:   1,
			conn:     sess,
		}
		if got := view.BaseTypeName(); got != "SINT" {
			t.Errorf("BaseTypeName = %q, want SINT (layer 3 size=1 inference)", got)
		}
	})

	t.Run("layer3_size_inference_2byte", func(t *testing.T) {
		sess := newViewTestSession()
		view := SymbolView{
			BaseType: ADSTBigType,
			DataType: "UnknownType",
			Length:   2,
			conn:     sess,
		}
		if got := view.BaseTypeName(); got != "INT" {
			t.Errorf("BaseTypeName = %q, want INT (layer 3 size=2 inference)", got)
		}
	})

	t.Run("layer3_refuses_4byte", func(t *testing.T) {
		// 4 bytes is ambiguous between DINT/REAL — must NOT infer.
		// Returns "" so caller surfaces a clear error.
		sess := newViewTestSession()
		view := SymbolView{
			BaseType: ADSTBigType,
			DataType: "UnknownType",
			Length:   4,
			conn:     sess,
		}
		if got := view.BaseTypeName(); got != "" {
			t.Errorf("BaseTypeName = %q, want \"\" (layer 3 refuses 4-byte to avoid REAL/DINT ambiguity)", got)
		}
	})

	t.Run("layer3_refuses_8byte", func(t *testing.T) {
		sess := newViewTestSession()
		view := SymbolView{
			BaseType: ADSTBigType,
			DataType: "UnknownType",
			Length:   8,
			conn:     sess,
		}
		if got := view.BaseTypeName(); got != "" {
			t.Errorf("BaseTypeName = %q, want \"\" (layer 3 refuses 8-byte LREAL/LINT ambiguity)", got)
		}
	})

	t.Run("nil_conn_falls_through_to_inference", func(t *testing.T) {
		// Detached SymbolView (no Session) — layer 2 silently skipped.
		view := SymbolView{
			BaseType: ADSTBigType,
			DataType: "UnknownType",
			Length:   2,
			conn:     nil,
		}
		if got := view.BaseTypeName(); got != "INT" {
			t.Errorf("BaseTypeName with nil conn = %q, want INT", got)
		}
	})
}

// TestBaseTypeName_UnresolvableWarnsOnceWithARemedy.
//
// The case the plugin fell into: a 4-byte user-defined type (a DINT-backed enum)
// with no datatype table. BaseTypeName has to return "" — 4 and 8 are ambiguous
// widths, and this getter has no context to fetch a table with, nor permission to,
// since a caller running with symbol loading off has declined that upload. What it
// must NOT do is return "" silently, which is what shipped the value downstream as
// an unconverted string with nothing in the log or the return to say why.
//
// Once per symbol, because a consumer may call this per sample.
func TestBaseTypeName_UnresolvableWarnsOnceWithARemedy(t *testing.T) {
	logs := &testLogHandler{}
	sess := newViewTestSession()
	sess.logger = slog.New(logs)

	// The latch lives on the cached symbol, so the view must correspond to one.
	const name = "MAIN.eMachineState"
	sess.cache.symbols[symbolKey(name)] = &symbol{
		FullName: name,
		DataType: "E_MachineState",
		BaseType: ADSTBigType,
		Length:   4,
	}
	view := SymbolView{
		FullName: name,
		BaseType: ADSTBigType,
		DataType: "E_MachineState",
		Length:   4,
		conn:     sess,
	}

	if got := view.BaseTypeName(); got != "" {
		t.Fatalf("BaseTypeName = %q, want \"\": a 4-byte width cannot be inferred", got)
	}

	rec := logs.findByMessage("cannot resolve the base type")
	if rec == nil {
		t.Fatal("no warning for an unresolvable base type — this is the silent case the plugin worked around")
	}
	if rec.Level != slog.LevelWarn {
		t.Errorf("level = %v, want Warn", rec.Level)
	}
	if got := rec.attr("symbol"); got != name {
		t.Errorf("symbol attr = %q, want %q", got, name)
	}
	if got := rec.attr("size"); got != "4" {
		t.Errorf("size attr = %q, want 4 — the width is the reason it cannot be inferred", got)
	}
	if hint := rec.attr("hint"); !strings.Contains(hint, "LoadSymbols") {
		t.Errorf("hint does not name the remedy: %q", hint)
	}

	// Called again — and again — it must stay quiet.
	for i := 0; i < 5; i++ {
		_ = view.BaseTypeName()
	}
	if n := logs.countByMessage("cannot resolve the base type"); n != 1 {
		t.Errorf("warning count = %d after 6 calls, want 1: a per-sample caller would flood the log", n)
	}
}

// TestBaseTypeName_ResolvableDoesNotWarn: the warning must fire only when the
// answer is genuinely unavailable, or it becomes noise of exactly the kind this
// change set exists to remove.
func TestBaseTypeName_ResolvableDoesNotWarn(t *testing.T) {
	tests := []struct {
		name     string
		baseType ADSDataType
		length   uint32
		table    map[string]SymbolUploadDataType
		want     string
	}{
		{name: "protocol base type", baseType: ADSTReal32, length: 4, want: "REAL"},
		{
			name: "datatype table", baseType: ADSTBigType, length: 4,
			table: map[string]SymbolUploadDataType{"MyAlias": {DataType: "DINT"}}, want: "DINT",
		},
		{name: "inferred from a 1-byte width", baseType: ADSTBigType, length: 1, want: "SINT"},
		{name: "inferred from a 2-byte width", baseType: ADSTBigType, length: 2, want: "INT"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			logs := &testLogHandler{}
			sess := newViewTestSession()
			sess.logger = slog.New(logs)
			if tc.table != nil {
				sess.cache.datatypes = tc.table
			}
			view := SymbolView{
				FullName: "MAIN.x", BaseType: tc.baseType, DataType: "MyAlias",
				Length: tc.length, conn: sess,
			}
			if got := view.BaseTypeName(); got != tc.want {
				t.Errorf("BaseTypeName = %q, want %q", got, tc.want)
			}
			if n := logs.countByMessage("cannot resolve the base type"); n != 0 {
				t.Errorf("warned %d times for a base type it could resolve", n)
			}
		})
	}
}

// TestWarnUnresolvedBaseType_SubscribeThenReadWarnsOnce: the warning is raised at
// subscribe time (where an operator can still load the datatype table before
// samples start arriving as unconverted strings) and the read path must then stay
// quiet — one latch serves both, or the fix trades one flood for another.
func TestWarnUnresolvedBaseType_SubscribeThenReadWarnsOnce(t *testing.T) {
	logs := &testLogHandler{}
	sess := newViewTestSession()
	sess.logger = slog.New(logs)

	const name = "MAIN.eState"
	sess.cache.symbols[symbolKey(name)] = &symbol{
		FullName: name, DataType: "E_State", BaseType: ADSTBigType, Length: 4,
	}

	// Subscribe-time call.
	sess.warnUnresolvedBaseType(name)
	if n := logs.countByMessage("cannot resolve the base type"); n != 1 {
		t.Fatalf("warnings after subscribe = %d, want 1", n)
	}

	// Read-path calls afterwards.
	view := SymbolView{FullName: name, DataType: "E_State", BaseType: ADSTBigType, Length: 4, conn: sess}
	for i := 0; i < 3; i++ {
		_ = view.BaseTypeName()
	}
	if n := logs.countByMessage("cannot resolve the base type"); n != 1 {
		t.Errorf("warnings after reads = %d, want 1: the subscribe-time latch must cover the read path", n)
	}
}

// TestWarnUnresolvedBaseType_QuietOnceTheTableArrives: resolvability is
// re-derived when the warning is considered, so a session that loads the datatype
// table before subscribing never warns about symbols the table explains.
func TestWarnUnresolvedBaseType_QuietOnceTheTableArrives(t *testing.T) {
	logs := &testLogHandler{}
	sess := newViewTestSession()
	sess.logger = slog.New(logs)

	const name = "MAIN.eState"
	sess.cache.symbols[symbolKey(name)] = &symbol{
		FullName: name, DataType: "E_State", BaseType: ADSTBigType, Length: 4,
	}
	sess.cache.datatypes = map[string]SymbolUploadDataType{"E_State": {DataType: "DINT"}}

	sess.warnUnresolvedBaseType(name)
	if n := logs.countByMessage("cannot resolve the base type"); n != 0 {
		t.Errorf("warned %d times although the datatype table resolves this symbol", n)
	}
}

// TestTestLogHandler_QualifiesGroups covers the capture helper itself: a handler
// that silently flattened groups could make an assertion pass on a key the real
// handler would have written as "group.key".
func TestTestLogHandler_QualifiesGroups(t *testing.T) {
	logs := &testLogHandler{}
	lg := slog.New(logs)

	lg.WithGroup("net").With("port", 48898).Info("dialed", "peer", "10.0.0.1")
	lg.Info("inline", slog.Group("drop", slog.Int("frames", 12)))
	lg.Info("emptyGroupKeyInlines", slog.Group("", slog.String("k", "v")))
	lg.Info("emptyGroupIgnored", slog.Group("g"))

	rec := logs.findByMessage("dialed")
	if rec == nil {
		t.Fatal("no record captured")
	}
	if got := rec.attr("net.port"); got != "48898" {
		t.Errorf("WithGroup+With key = %q, want net.port=48898 (attrs: %v)", got, rec.Attrs)
	}
	if got := rec.attr("net.peer"); got != "10.0.0.1" {
		t.Errorf("record attr under group = %q, want net.peer=10.0.0.1 (attrs: %v)", got, rec.Attrs)
	}

	inline := logs.findByMessage("inline")
	if got := inline.attr("drop.frames"); got != "12" {
		t.Errorf("slog.Group key = %q, want drop.frames=12 (attrs: %v)", got, inline.Attrs)
	}

	if got := logs.findByMessage("emptyGroupKeyInlines").attr("k"); got != "v" {
		t.Errorf("an empty group key must inline its attributes, got %q", got)
	}
	if r := logs.findByMessage("emptyGroupIgnored"); len(r.Attrs) != 0 {
		t.Errorf("a group with no attributes must be ignored, got %v", r.Attrs)
	}
}

// TestTestLogHandler_GroupQualifiesOnlyLaterAttrs covers the reverse call order
// of TestTestLogHandler_QualifiesGroups: a group opened after an attribute must
// not reach back and qualify it. A handler that applied the group prefix that
// happened to be open at Handle time would record "net.request_id" here, and a
// test asserting on "request_id" would then fail for a reason that exists only
// in the helper.
func TestTestLogHandler_GroupQualifiesOnlyLaterAttrs(t *testing.T) {
	logs := &testLogHandler{}
	lg := slog.New(logs)

	lg.With("request_id", "abc").WithGroup("net").With("port", 48898).Info("dialed")

	rec := logs.findByMessage("dialed")
	if rec == nil {
		t.Fatal("no record captured")
	}
	if got, want := rec.attr("request_id"), "abc"; got != want {
		t.Errorf("attr added before the group = %q, want request_id=%q (attrs: %v)", got, want, rec.Attrs)
	}
	if rec.hasAttr("net.request_id") {
		t.Errorf("a group must not qualify an attribute added before it (attrs: %v)", rec.Attrs)
	}
	if got, want := rec.attr("net.port"), "48898"; got != want {
		t.Errorf("attr added after the group = %q, want net.port=%q (attrs: %v)", got, want, rec.Attrs)
	}
}

// groupLogValuer is a slog.LogValuer whose LogValue returns a group, the case
// that separates resolving from not resolving.
type groupLogValuer struct{ port int }

func (g groupLogValuer) LogValue() slog.Value {
	return slog.GroupValue(slog.Int("port", g.port), slog.String("proto", "tcp"))
}

// TestTestLogHandler_ResolvesLogValuer: the capture helper must resolve values
// before inspecting their kind. secret in this package is a slog.LogValuer, so
// an unresolved value would be stored as one opaque scalar — and a redaction
// assertion could pass against text the real handler never wrote.
func TestTestLogHandler_ResolvesLogValuer(t *testing.T) {
	logs := &testLogHandler{}
	lg := slog.New(logs)

	lg.Info("valued", "conn", groupLogValuer{port: 48898})
	lg.Info("secret", "password", secret("hunter2"))

	rec := logs.findByMessage("valued")
	if rec == nil {
		t.Fatal("no record captured")
	}
	if got, want := rec.attr("conn.port"), "48898"; got != want {
		t.Errorf("LogValuer group child = %q, want conn.port=%q (attrs: %v)", got, want, rec.Attrs)
	}
	if got, want := rec.attr("conn.proto"), "tcp"; got != want {
		t.Errorf("LogValuer group child = %q, want conn.proto=%q (attrs: %v)", got, want, rec.Attrs)
	}
	if rec.hasAttr("conn") {
		t.Errorf("an unresolved LogValuer was stored as a scalar (attrs: %v)", rec.Attrs)
	}

	pw := logs.findByMessage("secret")
	if got, want := pw.attr("password"), "[REDACTED]"; got != want {
		t.Errorf("secret attr = %q, want %q (attrs: %v)", got, want, pw.Attrs)
	}
}
