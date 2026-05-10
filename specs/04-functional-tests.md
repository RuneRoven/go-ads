# Functional Test Specifications

Functional tests in Go (the project's native language). Each test traces back to a requirement (R-XXX-NNN). No coverage filler.

## Test types

| Type | Build tag | Lives in | Network | Run with |
|------|-----------|----------|---------|----------|
| Unit (T-U) | none | `*_test.go` | none | `go test ./...` |
| Functional (T-F) | none | `*_test.go` | net.Pipe / mock | `go test ./...` |
| Integration (T-I) | `integration` | `integration_test.go`, `testconn_integration_test.go` | real PLC | `go test -tags integration ./...` |
| Benchmark | none | `*_bench_test.go` (currently absent) | mock | `go test -bench` |

Unit and functional tests SHALL pass on every commit. Integration tests SHALL pass before any release tag.

## Naming convention

`Test<Subject><Scenario>` for ID-bearing tests:

- `TestAddSymbolNotification_DuplicateRejected` — validates R-NOT-002.
- `TestSumRead_PerItemUndersizeCascades` — validates R-SUM-006.
- `TestSymbolView_SnapshotConsistency` — validates R-VIEW-001.

Scenarios are concrete: `_DuplicateRejected`, `_TransportErrorAllSkipped`, `_GenerationBumpDetected`. NOT `_Works`, `_Basic`, `_Happy`.

Each test file SHALL have a leading comment listing the requirements covered:

```go
// Tests in this file validate:
//   R-NOT-001, R-NOT-002, R-NOT-003, R-NOT-004, R-NOT-005, R-NOT-013
```

## Traceability format

Every test has a comment block immediately above its function:

```go
// T-U-104 — TOCTOU re-check on duplicate-subscribe race.
// Validates: R-NOT-003.
// Mechanism: pre-check passes (no existing config); concurrent goroutine
// inserts a config under notifications.lock during this caller's PLC roundtrip;
// post-roundtrip re-check detects the duplicate, releases the just-acquired
// PLC handle, returns error.
// Hardware: synthetic.
func TestAddSymbolNotification_PostRoundtripDuplicateRace(t *testing.T) {
    ...
}
```

The traceability matrix is built by `grep -E '^// T-[UFI]-[0-9]+'` across the test files. CI SHALL fail if any T-X-NNN ID is referenced in `01-requirements.md` but absent from the test files (gap detection).

## Test ID assignment

- `T-U-001..T-U-099` — Connection lifecycle (CON)
- `T-U-100..T-U-199` — Notifications (NOT)
- `T-U-200..T-U-299` — Symbol cache (CACHE)
- `T-U-300..T-U-399` — Symbol type (SYM)
- `T-U-400..T-U-499` — SymbolView (VIEW)
- `T-U-500..T-U-599` — Transport (TX)
- `T-U-600..T-U-699` — Reconnect (RECON)
- `T-U-700..T-U-799` — ADS commands (CMD)
- `T-U-800..T-U-899` — Sum commands (SUM)
- `T-U-900..T-U-999` — Route registration (ROUTE)
- `T-U-1000..T-U-1099` — Parse (PARSE)
- `T-U-1100..T-U-1199` — Lock invariants (LOCK)
- `T-DOC-NNN` — godoc-content tests (require comment matches the contract)

Functional tests (`T-F-NNN`) follow the same module split with `T-F-` prefix where the test crosses into pipeline behavior (e.g. listen + worker pool + handleNotification end-to-end with mock TCP).

Integration tests (`T-I-NNN`) follow same prefix with `T-I-`.

## Anti-patterns

Refused at review (per §`02-quality-constitution.md`):

### A1 — Coverage filler

```go
// BAD
func TestReadFromSymbol(t *testing.T) {
    conn := setupConnection(t)
    _, err := conn.ReadFromSymbol("foo")
    if err != nil {
        t.Fatal(err)
    }
}
```

This test passes when `ReadFromSymbol` returns ANY non-error result. It tests "does this function not crash". It cannot detect: wrong value, stale value, type-misparse, bound violation, lock violation. Refused.

### A2 — Mock-only with constant fixture

```go
// BAD
func TestParseInt32(t *testing.T) {
    sym := &Symbol{DataType: "DINT", Length: 4}
    data := []byte{0x01, 0x00, 0x00, 0x00}
    val, _ := sym.parse(data, 0, nil)
    if val != "1" { t.Fatal(...) }
}
```

This test exercises one byte sequence. It doesn't catch: unsigned interpretation bug for negative inputs, off-by-one, type-name mismatch (DINT vs UDINT). Use table-driven cases at minimum.

### A3 — Snapshot test with no requirement

```go
// BAD
func TestNotificationFormat(t *testing.T) {
    expect, _ := os.ReadFile("testdata/notif.golden")
    got := buildNotification(...)
    if !bytes.Equal(got, expect) { t.Fatal(...) }
}
```

Without a requirement saying which fields the test should preserve, the snapshot locks current behavior including bugs. If the snapshot is meaningful, document the requirement first; if not, replace with a structural test.

## Required tests (traceability policy)

The previous incarnation of this section listed ~56 aspirational `T-U-NNN` test names mapped to R-IDs. An audit on 2026-05-10 (`specs/11-spec-code-audit.md` Axis 3-F + Action Plan #20) found only 3 of those 56 names actually existed in the test files; the others either (a) were never written, or (b) were written under different names. The "T-U-NNN <-> test function name" registry was therefore not load-bearing for traceability.

Active policy (2026-05-10 onward):

- Each test SHALL have a `// Validates: R-XXX-NNN[, R-YYY-MMM ...]` comment immediately above the function. `NO-SPEC` is permitted only for regression-guards that have no requirement anchor.
- `T-U-NNN` numeric IDs are NOT required and SHALL NOT be added retroactively. New tests that have a clear bucket MAY use the assignment ranges below for organizational clarity, but absence of the prefix is not a defect.
- Cross-checks: a CI step (or manual `grep -r "// Validates: R-"`) verifies every requirement with a non-empty `Verification:` field has at least one matching test comment. Requirements with `Verification:` fields citing T-U-NNN are scored against tests via the `// Validates:` comment, NOT against name match.

Snapshot at 2026-05-10:

- 134 `// Validates: R-XXX-NNN` comments across 13 test files; 60 unique R-IDs anchored.
- 5 `symbolSumAddress` tests in `symbols_test.go` re-anchored to R-SUM-008 (Action Plan #19).
- ~25 NO-SPEC regression guards remain by design (defs_test.go format helpers, codec edge cases) — see `specs/11-spec-code-audit.md` Axis 2 for the keep/backfill/delete decisions.

## Existing tests audit

(`pre-spec`): existing tests in `*_test.go` were written without traceability comments. Backfill task:

1. For each existing test, add a `// T-X-NNN — <one-line> / Validates: R-...` comment.
2. Drop tests that don't trace to any requirement (file under `02-quality-constitution.md` A1 — coverage filler).
3. Identify tests that would be promoted to canonical seed list; rename to match the convention.

This audit is a prerequisite for using the spec for review.

## CI integration

`.github/workflows/ci.yml` (or equivalent) SHALL:

1. Run `go test -count=1 -race ./...` — must pass.
2. Run `golangci-lint run ./...` — must exit zero.
3. Run `scripts/verify-traceability.sh` — checks every R-XXX-NNN with non-empty `Verification:` field has at least one matching `T-X-NNN` comment in test files.
4. Skip integration tests in CI by default; release jobs run on a tag with hardware-test runners.
