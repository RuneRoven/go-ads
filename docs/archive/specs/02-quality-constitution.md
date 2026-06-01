# go-ads Quality Constitution

What "correct" means for this specific project. This document is opinionated; it embeds the values that should guide every PR review and code change.

## Project context

go-ads is a **protocol library** for industrial automation. Failure modes:

- **Silent data corruption** is the worst outcome. A library that returns wrong data to a control loop can damage equipment, cause safety incidents, or yield bad manufacturing. Loud errors > quiet wrong answers.
- **Stuck connections** cause downtime. A reconnect that never succeeds, or a goroutine deadlock, prevents data flowing to the upper layers (typically benthos pipelines feeding databases / dashboards).
- **Resource leaks** (handles, goroutines, sockets) accumulate over hours/days; matter for long-running services even if invisible in tests.
- **Performance regressions** matter once the library is in front of high-rate notifications (>1000/s) or large symbol tables (50k+ symbols).

Order of importance: **correctness > liveness > resource discipline > performance > ergonomics**.

## What "correct" means

A change is correct if it:

1. **Satisfies the requirements in `specs/01-requirements.md`.** Every behavior the library exhibits must trace back to a requirement; behaviors with no requirement are either undocumented features (write the requirement) or accidents (file a bug).
2. **Does not regress hardware tests.** TC2 + TC3 smoke tests pass after the change. If a change requires test updates, the new test version must reflect the same user outcome (not a weaker one).
3. **Passes `go test -race`** on the changed packages, with a NEW test exercising the changed code path. The race detector catches what code review misses.
4. **Passes `golangci-lint run ./...`** with no new disabled rules.
5. **Has an inline rationale where the next reader will need one.** A clever piece of code with no comment is a future bug.
6. **Names communicate intent.** Variable names are nouns, methods are verbs, lock names describe what they protect (`cache.lock` not `mu1`). Initialisms in caps (`ADSState`, `AMSAddress`).

## Anti-patterns to refuse

### Coverage theater

A high `go test -cover` percentage with no test that would catch a real bug.

**Refuse**:
- Tests that only assert `err == nil` with no value verification.
- Tests that mock the network and run a constant-fixture path (the wire decoder is what's under test, not the channel plumbing).
- Tests that reuse the same fixture across many cases without exercising parsing edge cases (zero length, oversize length, NUL in middle, surrogate pairs).
- Snapshot tests that lock in current behavior without explaining the requirement.

**Prefer**:
- Tests that fail on unpatched code AND pass on patched code (red-green; see `specs/06-tdd-protocol.md`).
- Tests with a comment saying which requirement they verify (`// validates R-NOT-005`).
- Tests with deterministic synthetic inputs that exercise edge cases (zero length, max length, surrogate pair at truncation point, generation bump mid-roundtrip).

### Defensive programming without specification

Adding nil-checks, depth caps, and retries because "it might break someday" without describing the failure mode the defense protects against.

**Refuse**:
- Adding nil-checks for parameters where the type system or contract guarantees non-nil.
- Adding retries for errors the library can't distinguish (catch-all retry loops).
- Adding goroutines or worker pools for theoretical scaling without a load profile.

**Prefer**:
- Defenses backed by either a reproducible bug (B-NNN), a wire-protocol surprise (PROTOCOL.md cite), or an explicit threat model (R-XXX-NNN with priority CRITICAL/HIGH).
- Each defense has a 1-2 line rationale comment naming the scenario.
- Each depth cap, timeout, and retry count is named (constant, not magic number) and documented.

### Premature abstraction

Extracting a helper or interface before the duplication is at least 3-fold or before the second use site has materialized.

**Refuse**:
- Helpers extracted from a single use site for "future flexibility".
- Interfaces declared with one implementation.
- Generic types where concrete types would do.

**Prefer**:
- Inline duplication until the third site appears.
- Helpers extracted only after a real second use case demands it.
- Generic types only where the cost of duplicating logic across types exceeds the cognitive cost of generics (sumCmdSpec is a borderline case; documented in R-SUM-007).

### Speculative versioning / configuration knobs

Adding options, configuration parameters, or version flags without a known consumer that needs them.

**Refuse**:
- Options whose default is the only value any caller should use.
- Options that toggle behavior the wire protocol determines (e.g. forcing TC3-style behavior on TC2 — the protocol decides, not the user).

**Prefer**:
- Options that map to a real use-case constraint (Docker NAT → `WithHostIP`, persistent vs ephemeral routes → `WithForceRouteRegistration`).
- Defaults that match the most common deployment.

### Comment rot

Comments that describe what the code did before, or what an old bug fix achieved.

**Refuse**:
- Comments referencing F-XX bug-fix tracking IDs after Plan-C 4 (already removed).
- Comments referencing "the previous version" without saying what was wrong with it.
- Comments restating the code (`// loop over results` above `for ... range results`).

**Prefer**:
- Comments stating the WHY (the constraint the code satisfies, the invariant it preserves).
- Citations to PROTOCOL.md§Section or `R-XXX-NNN` for spec-mandated behavior.
- Inline rationale for non-obvious choices, with concrete failure mode if the choice is wrong.

## Fitness-to-purpose scenarios

Pre-merge, walk the change through each of these scenarios and confirm correctness:

### S1 — Single-symbol read on healthy TC3
Caller does `LoadSymbols`, then `ReadFromSymbol("MAIN.foo")`. Connection is fresh. Return the parsed value.
- Hits: R-CACHE-001, R-CACHE-007, R-CMD-002, R-PARSE-001.
- Failure mode: wrong value parsed; check R-PARSE-006 (oversize), R-PARSE-007 (type-specific).

### S2 — High-rate notifications on TC3
1000 notifications/sec for 60s. No drops in user channel (assuming buffered ≥1k); no goroutine count growth; cache.lock contention bounded.
- Hits: R-NOT-001, R-NOT-006, R-TX-006, R-LOCK-002.
- Failure mode: goroutine leak (fixed in Round 4); user channel saturates → drops surfaced (acceptable).

### S3 — Online change on TC3 mid-notification
PLC operator triggers online change while a notification subscription is active for a removed symbol.
- Hits: R-NOT-005 (re-resolve via FullName), R-CACHE-003 (epoch bump), R-CACHE-004 (zeroOldSymbolHandles), R-NOT-013 (resubscribe retry).
- Failure mode: stale Symbol pointer parsed; live cache shows different value; ReadFromSymbol returns "" while notification dispatches into orphan.

### S4 — TCP cable unplug for 13s, replug
TCP keepalive (Idle=3s, Interval=2s, Count=5) detects dead connection within ~13s. Reconnect launches; route probed; symbols reloaded; notifications re-subscribed.
- Hits: R-RECON-001, R-RECON-005, R-RECON-006, R-NOT-014.
- Failure mode: wedged in retry loop; double-disconnect callback (fixed by R-SES-008); strict-mode failures.

### S5 — TC2 (older runtime) batch notification
TC2 doesn't support 0xF085/F086. `AddSymbolNotifications` falls back to per-item `AddDeviceNotification`.
- Hits: R-SUM-003 (per-opcode state), R-SUM-001..R-SUM-002 (fallback).
- Failure mode: shared-state poisoning (the original Round-1 critical), individual fallback logic divergence.

### S6 — Concurrent `LoadSymbols` during `AddSymbolNotification`
User code calls `LoadSymbols` from goroutine A while goroutine B is in `AddSymbolNotification` mid-roundtrip.
- Hits: R-NOT-004 (epoch re-check), R-NOT-005 (handleNotification re-resolve), R-CACHE-008 (lock ordering).
- Failure mode: stranded *Symbol pointer; ReadFromSymbol vs notification dispatch divergence (fixed in Round 3+4).

### S7 — Goroutine leak on Connect failure mid-handshake
`Connect` succeeds at TCP dial, fails at local handshake. The listen/transmit/recvWorker goroutines must exit cleanly so the user can retry Connect on the same Session or call `Close`.
- Hits: R-SES-002, R-SES-005, R-RECON-008.
- Failure mode: dangling goroutines; "sync: WaitGroup misuse" panic on retry (fixed by Plan-B F-02).

### S8 — `SymbolView` outliving Session close
User obtains SymbolView from `GetSymbol`, then `sess.Close()`. User reads `view.Value`.
- Hits: R-VIEW-001 (snapshot semantics), R-VIEW-007 (post-Close behavior).
- Failure mode: nil-deref if Value were a method touching `conn.cache`; current snapshot model returns the captured value safely.

### S9 — Adversarial PLC sends oversized AMS-TCP packet
A misbehaving or malicious PLC sends a TCP packet with `Length = 0xFFFFFFFF` (4 GiB).
- Hits: R-TX-003 (4 MiB sanity cap), R-PARSE-006.
- Failure mode: OOM allocation; stack overflow on parse.

### S10 — `bench` symbol with 256-deep struct nesting (synthetic)
PLC datatype response with 256-level deep nesting. Library SHALL NOT stack-overflow.
- Hits: R-SYM-002 (depth caps).
- Failure mode: stack overflow during loadSymbols.

## Test grades

When reviewing tests, grade by:

- **A** — would catch a real regression: covers a documented invariant; uses synthetic inputs that exercise edge cases; would fail on the bug it was added for; tests one thing.
- **B** — exercises the path: invokes the function, asserts something non-trivial. Covers happy path. Won't catch all regressions but better than nothing.
- **C** — coverage filler: invokes the function, asserts it returned without error or that count > 0. Adds to `go test -cover` but unlikely to catch any real bug.
- **F** — coverage theater: passes regardless of bugs. Rejected from review.

Grade-A tests are non-negotiable for `priority: CRITICAL` requirements.

## When to update this constitution

This document is opinionated; it can be wrong. Update when:

- A new failure mode is observed that this document doesn't address (incident → new fitness-to-purpose scenario).
- A "refuse" pattern turns out to be necessary (document the exception with a rationale).
- A new value supersedes an old one (e.g., if performance becomes more important than ergonomics, update the order in §What "correct" means).

Updates require a PR explicitly amending the constitution; not a slip-in change.

## Release & CI policy (R-NFR-NNN)

These entries govern release process and CI policy, not library runtime behaviour. They were relocated from `01-requirements.md` (where they did not fit the "behavioural requirements" frame) to keep the requirements doc focused on what the code actually does.

### R-NFR-001 — Backwards-incompatible changes need version bump
- **Priority**: HIGH
- **Source**: SemVer
- **Statement**: Pre-1.0: breaking changes are allowed but MUST be documented in CHANGELOG.md and the commit message MUST include `!` per Conventional Commits. v2.x is acceptable for the current trajectory; v3.0 reserved for the next major.

### R-NFR-002 — Hardware tests required for release
- **Priority**: CRITICAL
- **Source**: code-as-spec
- **Statement**: A release SHALL pass the smoke test (`Connect`, `ReadSymbol`, `Write`, `Notification`, `Reconnect`, `BatchNotification`) on both TC2 and TC3 hardware before tagging.

### R-NFR-003 — Linter clean
- **Priority**: HIGH
- **Source**: code-as-spec
- **Statement**: `golangci-lint run ./...` SHALL exit zero on every commit. Disabled lints documented in `.golangci.yml` with rationale.

### R-NFR-004 — Deterministic unit tests
- **Priority**: HIGH
- **Source**: code-as-spec
- **Statement**: Unit tests (no `//go:build integration`) SHALL be deterministic — no real network I/O, no PLC dependency, no time.Sleep without explicit synchronization. Mocks/fixtures fine.
