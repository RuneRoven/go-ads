# go-ads Specifications

Behavioral specifications for the go-ads Beckhoff TwinCAT ADS protocol library. The codebase has accumulated complex concurrency behavior across multiple bug-fix campaigns; these specs are the source of truth that future code, reviews, and tests must satisfy.

## Reading order

1. **`00-INDEX.md`** (this file) — navigation, ID conventions, glossary.
2. **`08-usage-patterns.md`** — concrete user usage + real PLC constraints. **READ FIRST** before `01`. Every requirement must trace to a scenario here.
3. **`01-requirements.md`** — behavioral requirements (R-MODULE-NNN). Foundation document; everything else traces back to this.
4. **`02-quality-constitution.md`** — what "correct" means for this project. Fitness-to-purpose scenarios. Coverage-theater prevention.
5. **`03-review-protocol.md`** — 3-pass code review protocol. Each pass finds bugs the others can't.
6. **`04-functional-tests.md`** — unit + functional tests in Go, traced to requirements.
7. **`05-integration-tests.md`** — E2E hardware tests on real PLCs (TC2 + TC3) grounded in use cases.
8. **`06-tdd-protocol.md`** — red-green verification protocol for confirmed bugs.
9. **`07-bug-report.md`** — consolidated bug report; every finding has spec basis, repro, patch, regression test.

## Cardinal rules

1. **No assumptions**. When a behavior is unknown, mark `INVESTIGATION-NEEDED` and stop. Investigation outcomes go in the log at the end of `08-usage-patterns.md`.
2. **Real scenarios only**. Every defensive code path SHALL trace to a concrete user scenario or PLC behavior in `08-usage-patterns.md`. Defenses for fantasy scenarios are rejected at review.
3. **Spec drives code**. New behavior requires a new requirement first; new requirement requires a real scenario first.

Adjacent (existing) docs:

- **`PROTOCOL.md`** — wire-protocol reference (frame layouts, opcodes, command IDs, return codes). Cited from spec.
- **`IMPLEMENTATION.md`** — architecture, concurrency model, sub-type design. Cited from spec.
- **`README.md`** — user-facing API guide.

The specs in this directory describe **behavior**; PROTOCOL.md describes **wire format**; IMPLEMENTATION.md describes **structure**. Together they define the project.

## ID conventions

### Requirements: `R-<MODULE>-<NNN>`

Modules:

| Module  | Scope                                                |
|---------|------------------------------------------------------|
| `SES`   | Session lifecycle (Connect, Close, IsDisconnected, callbacks, options) — managed Layer 3 |
| `CL`    | Client (raw RPC layer 2): Dial, Close, transport-down, notification + on-drop callbacks |
| `NOT`   | Notification subscribe/dispatch/unsubscribe          |
| `CACHE` | Symbol cache lifecycle (load, swap, on-demand, generation) |
| `SYM`   | `Symbol` internal type semantics                     |
| `TX`    | Transport layer (TCP socket, framing, request/response mux) |
| `RECON` | Reconnect FSM (trigger, retry, resubscribe, generation) |
| `CMD`   | ADS command handlers (Read, Write, ReadDeviceInfo, ReadState, …) |
| `SUM`   | Sum/batch commands (SumRead, SumWrite, SumAddNotif, SumDeleteNotif) |
| `ROUTE` | UDP route registration (port 48899, invokeID echo)   |
| `VIEW`  | `SymbolView` external API (snapshot semantics, IsValid, Children, ChildrenWalk) |
| `PARSE` | Wire parsing (parse, writeToNode, type tables, surrogate handling) |
| `LOCK`  | Cross-cutting concurrency invariants (lock ordering, generation, atomic guarantees) |

Each requirement entry contains:

- **Priority**: `CRITICAL` (correctness/safety, must satisfy) / `HIGH` (correctness, expected behavior) / `MEDIUM` (robustness/usability) / `LOW` (cosmetic, ergonomics)
- **Source**: `PROTOCOL.md§N` / `Beckhoff InfoSys` / `IEC 61131-3` / `code-as-spec` / `community` / `incident`
- **Statement**: imperative SHALL/MUST language
- **Invariants**: machine-checkable conditions
- **Verification**: test ID(s) that prove satisfaction (`T-U-NNN` / `T-F-NNN` / `T-I-NNN`) — empty if not yet covered
- **Origin**: which review round / commit introduced/last-touched

### Tests: `T-<TYPE>-<NNN>`

| Type | Meaning                  | Lives in                              |
|------|--------------------------|---------------------------------------|
| `U`  | Unit (synchronous, mock) | `*_test.go` (no build tag)            |
| `F`  | Functional (in-process)  | `*_test.go` (no build tag)            |
| `I`  | Integration (real PLC)   | `integration_test.go` (`//go:build integration`) |
| `H`  | Hardware-only smoke      | `integration_test.go` smoke set       |

Each test entry contains:

- **Validates**: list of `R-XXX-NNN` requirements
- **Mechanism**: what the test exercises (specific code path, race condition, edge case)
- **Bug origin**: B-NNN if regression test for a confirmed bug
- **Hardware**: TC2 / TC3 / both / synthetic (no PLC)

### Bugs: `B-<NNN>`

Each bug entry contains:

- **Severity**: `CRITICAL` / `HIGH` / `MEDIUM` / `LOW`
- **Spec basis**: R-XXX-NNN that the buggy code violates
- **Discovery**: review round + agent (`R3 Errors+Concurrency`, `R4 Devil's Advocate`, `Qwen3-Coder validation`, etc.)
- **Reproduction**: synthetic or hardware repro steps
- **Patch**: commit SHA(s)
- **Regression test**: T-X-NNN that fails on unpatched code, passes on patched

## Glossary

| Term       | Meaning                                                                |
|------------|------------------------------------------------------------------------|
| ADS        | Automation Device Specification — Beckhoff's L7 protocol over TCP/UDP. |
| AMS        | Advanced Message Service — addressing layer (NetID + port).            |
| AMSAddress | 8-byte tuple: 6-byte NetID + 2-byte port.                              |
| NetID      | 6-byte AMS host identifier (typically `IP.1.1`).                       |
| TC2        | TwinCAT 2 runtime (older, single-threaded, port 801 typical).          |
| TC3        | TwinCAT 3 runtime (newer, multi-threaded, port 851 typical).           |
| PLC        | Programmable Logic Controller (the device hosting the runtime).        |
| Symbol     | Named PLC variable with type, address, and metadata.                   |
| Handle     | 32-bit PLC-side identifier for a symbol or notification subscription.  |
| Notification | PLC-pushed value update (cyclic or on-change).                       |
| Sumup      | Beckhoff term for batch operations (multiple read/write/notify in one round-trip). |
| Online change | TC3 feature: PLC code reloaded without restart; symbol table may change. |
| Route      | AMS-level address mapping registered on the PLC for the client's NetID. |
| InvokeID   | 32-bit per-request identifier for response correlation.                |
| F-XX       | Pre-1.0 bug-fix tracking IDs from the original triage doc (Plan-B). Removed from inline comments in Plan-C 4. |

## Spec change protocol

Specs are **living documents**. Changes follow:

1. Open issue describing the requirement change with rationale.
2. Edit the relevant spec file; bump R-XXX-NNN if behavior changes (do NOT renumber existing IDs).
3. Update tests (T-XXX-NNN) to match new behavior.
4. Update bug report (B-NNN) if change closes/opens a finding.
5. Run `go test -race`, `golangci-lint`, hardware smoke; document in PR.
6. Cite spec ID(s) in the commit message: `feat(R-NOT-007): debounce duplicate-handle warnings`.

Spec violations found post-merge are bugs (file under B-NNN); spec ambiguities found are requirement gaps (add new R-XXX-NNN with `priority: MEDIUM`).
