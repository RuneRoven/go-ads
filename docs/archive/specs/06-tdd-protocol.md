# TDD Red-Green Verification Protocol

For every confirmed bug, the regression test MUST fail on unpatched code AND pass on patched code. This document defines the exact procedure.

## Why red-green matters

A test that passes on patched code but never failed on unpatched code is **coverage theater** (per `02-quality-constitution.md`). It might be exercising a path adjacent to the bug; it might not catch a regression if the bug returns. The only way to know the test guards the fix is to see it fail without the fix.

For protocol libraries, regressions are silent (wrong byte parsed, missed notification, leaked goroutine). Catching them in CI before they ship matters.

## Procedure (per bug)

For every confirmed bug B-NNN:

### 1. Reproduce on unpatched code

Check out the commit immediately BEFORE the fix:

```bash
git log --oneline | grep '<bug fix description>'
git checkout <fix-commit>~1
```

Or stash fix changes:

```bash
git stash
```

Build a synthetic test (or hardware scenario) that exposes the bug. The test SHALL be DETERMINISTIC — same inputs, same outputs every run. Hardware tests are second-best for reproduction; prefer synthetic.

Run the test. Confirm it FAILS with a clear diagnostic that matches the bug description.

```bash
go test -run 'TestRegression_<BugName>' -v -count=1 ./...
```

Capture the failure output verbatim in `07-bug-report.md` under `## Reproduction`.

### 2. Apply the fix

Pop the stash or check out the fix commit:

```bash
git stash pop
# or
git checkout <fix-commit>
```

Re-run the test. It MUST PASS.

```bash
go test -run 'TestRegression_<BugName>' -v -count=1 ./...
```

### 3. Confirm release-set tests still pass

Run the entire test suite to confirm the fix didn't regress anything else:

```bash
go test -count=1 -race ./...
golangci-lint run ./...
```

### 4. Hardware verification (if applicable)

If the bug has a hardware dimension (race condition observable only with real PLC timing), run the relevant integration test on TC2 + TC3:

```bash
set -a && source .env.integration.224 && set +a && \
  sudo -E go test -tags integration -run '<RelevantHardwareTest>' -v -count=1
```

### 5. Document in spec

Add the regression test ID to:

- `01-requirements.md` — under `Verification:` of the requirement R-XXX-NNN that the bug violated.
- `07-bug-report.md` — under `Regression test:` of the bug entry.
- The test file itself: `// T-U-NNN — regression for B-NNN. ...`

## Required artifacts per bug

Every bug entry in `07-bug-report.md` SHALL have:

```
B-NNN — <one-line summary>
- Severity: CRITICAL | HIGH | MEDIUM | LOW
- Spec basis: R-XXX-NNN (which requirement was violated)
- Discovery: <round + agent + date>
- Reproduction:
  ```
  <commands or scenario to trigger the bug on unpatched code>
  Expected: <what should happen>
  Actual: <what happened>
  ```
- Patch: <commit-sha> "<commit-subject>"
- Regression test: T-X-NNN (must fail on unpatched, pass on patched)
- Verified red-green: yes | pending
```

The `Verified red-green: yes` field is the gate. A bug is not closed without it.

## Examples

### B-001 (illustrative) — handleNotification stranded-symbol parse

**Bug**: `handleNotification` used `*Symbol` from `notifs.activeNotifications`, which after a concurrent `loadSymbols` swap was orphaned. `parse(content, 0, conn.cache.datatypes)` ran with a fresh-table `datatypes` against a stranded-symbol's `DataType` key — type mismatch silently wrote into orphaned memory.

**Spec violated**: R-NOT-005 (handleNotification re-resolves symbol via FullName).

**Reproduction (unpatched)**:

```go
// T-U-106 (regression for B-001)
func TestHandleNotification_StrandedSymbolReResolves(t *testing.T) {
    conn := newTestConnection()
    defer conn.lifecycle.shutdown()

    // Initial cache: symbol "foo" of type INT
    sym1 := &Symbol{FullName: "foo", DataType: "INT", Length: 2}
    conn.cache.symbols[symbolKey("foo")] = sym1
    conn.cache.datatypes = map[string]SymbolUploadDataType{}

    // Subscribe (synthetic)
    ch := make(chan *Update, 1)
    sym1.Notification = ch
    conn.notifs.activeNotifications[42] = sym1

    // Concurrent loadSymbols swap: replace "foo" with same name but DataType "DINT"
    // (online change) and bump generation
    sym2 := &Symbol{FullName: "foo", DataType: "DINT", Length: 4}
    conn.cache.symbols = map[string]*Symbol{symbolKey("foo"): sym2}
    conn.cache.generation.Inc()

    // Inbound notification 4-byte data (DINT-shaped) for handle 42
    data := []byte{0x10, 0x00, 0x00, 0x00}

    // Handle the notification
    conn.handleNotification(conn.lifecycle.ctx, 42, 0, data)

    select {
    case u := <-ch:
        // Pre-fix: parse used sym1 (INT) on 4-byte input → 2-byte misread,
        // returns "16" for INT instead of "16" for DINT. Plausible value, but the
        // wrong Symbol's Value is updated.
        if sym2.Value == "" {
            t.Error("live symbol's Value was not updated; pre-fix bug")
        }
        if sym1.Value != "" {
            t.Error("orphaned symbol's Value was updated (pre-fix bug)")
        }
        _ = u
    case <-time.After(time.Second):
        t.Fatal("no notification delivered")
    }
}
```

Expected pre-fix behavior: `sym1.Value` is set, `sym2.Value` stays empty (silent divergence).

Expected post-fix behavior: `sym2.Value` is set (handleNotification re-resolved via FullName under cache.lock), `sym1.Value` stays empty.

**Patch**: cmd_notification.go re-resolves via `conn.cache.symbols[symbolKey(fullName)]` under cache.lock; logs Warn if symbol absent.

**Regression test**: T-U-106 above.

**Verified red-green**: pending until applied with this protocol.

### B-002 (illustrative) — Sum capability state poisoning

**Bug**: `SumAddDeviceNotification` and `SumDeleteDeviceNotification` shared a single `sumNotifState` atomic. PLC supports 0xF085 (Add) but not 0xF086 (Delete). After SumAdd succeeds (state CAS to 1=supported), SumDelete probe fails (CAS-from-0 fails, state stays 1, code skips fallback). Or vice versa.

**Spec violated**: R-SUM-003 (per-opcode state required).

**Reproduction (unpatched)**:

```go
// T-U-800 (regression for B-002)
func TestCapabilities_AddDeleteNotifStatesIndependent(t *testing.T) {
    conn := newTestConnection()
    defer conn.lifecycle.shutdown()

    // PLC supports 0xF085 but not 0xF086 (synthetic)
    // Pre-fix: shared state means after Add CAS-to-1, Delete sees state=1 and
    // skips its own probe + fallback.
    conn.capabilities.SumAddNotifStateStore(1) // mark Add as supported
    if conn.capabilities.SumDeleteNotifStateLoad() != 0 {
        t.Errorf("Delete state should be independent (still 0); got %d (pre-fix bug)",
            conn.capabilities.SumDeleteNotifStateLoad())
    }
}
```

Expected pre-fix: `SumDeleteNotifStateLoad() == 1` (shared state poisoned).

Expected post-fix: `SumDeleteNotifStateLoad() == 0` (independent fields).

**Patch**: capabilities.go split into `sumAddNotifState` + `sumDeleteNotifState` atomic.Uint32 fields; cmd_sum.go calls per-opcode state methods.

**Regression test**: T-U-800.

**Verified red-green**: yes (commit `919d2b2`).

## Backfill task

For each bug in `07-bug-report.md` (rounds 1-4), apply the protocol:

1. Stash current state.
2. Check out parent of fix commit.
3. Build a synthetic regression test.
4. Verify failure on unpatched code.
5. Restore + verify pass.
6. Update `Verified red-green:` field.

Bugs without applicable synthetic reproduction (e.g. lock-ordering bugs that need adversarial timing) MAY use a property-based test or stress test; the requirement is that the test would catch a future regression.

## CI integration

Beyond `go test -count=1 -race`:

- A new CI job SHALL run each `TestRegression_*` test individually with verbose output, capturing the test's runtime and any flakes.
- A regression test that flakes (passes sometimes, fails sometimes) is broken; fix the test or the underlying flaky behavior.
- Slow regression tests (> 5s) require explicit timing budget approval.

## Anti-patterns

- **A regression test that always passes** — useless. Verify red-green.
- **A regression test referencing internal fields not the requirement** — fragile. Reference the contract (R-XXX-NNN), not the implementation.
- **A regression test that requires hardware to fail** — limits CI utility. Prefer synthetic; hardware-only is OK as a complement, not the only test.
