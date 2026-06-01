# Consolidated Bug Report

Every confirmed finding from review rounds 1-4 with: spec basis, severity, reproduction, patch reference, regression test ID, red-green status.

Rounds 1-3 fixes have been merged onto `dev` (single squash commit `0ce533d`). Round 4 fixes are on branch `review/round4-fixes` (commit `31ac1c9`), pending re-validation against this spec before merge.

A bug is closed when:

1. The patch is in `dev`.
2. A regression test (T-X-NNN) exists.
3. The regression test has been verified RED on unpatched code, GREEN on patched code (per `06-tdd-protocol.md`).
4. The corresponding requirement (R-XXX-NNN) lists the test ID under `Verification:`.

A bug whose patch is merged but lacks regression-test verification is in **PARTIAL** status; close-out is a backfill task.

## Severity tags

- **CRITICAL** — silent data corruption, deadlock, security issue
- **HIGH** — observable wrong behavior, resource leak under load, lock-ordering hazard
- **MEDIUM** — robustness gap, edge-case failure, log noise
- **LOW** — cosmetic, minor inconsistency

---

## Round 1: Plan-B Phase 1-5 (initial bug-fix campaign, F-01..F-34)

These were tracked as F-XX in the original triage; the labels were stripped from inline comments in Plan-C 4. Mapped here to spec.

### B-001 Goroutine leak on local handshake failure
- **Severity**: HIGH
- **Spec basis**: R-SES-002, R-RECON-008
- **Discovery**: Plan-B triage (multi-agent + Qwen3-Coder + CodeRabbit), 2026-05-05
- **Patch**: `398d65d` "Fix: 1. Goroutine leak on local handshake failure..."
- **Regression test**: T-U-602 (pending backfill)
- **Verified red-green**: pending
- **Notes**: Original F-01 + F-02. Extracted `cleanupAfterFailedConnect` (later inlined in Plan-C 1.5).

### B-002 onDisconnect fires twice
- **Severity**: HIGH
- **Spec basis**: R-SES-008
- **Discovery**: Plan-B triage
- **Patch**: `398d65d` (CompareAndSwap gate added)
- **Regression test**: T-U-018 (pending)
- **Verified red-green**: pending

### B-003 Handle leak in GetSymbol on-demand path
- **Severity**: MEDIUM
- **Spec basis**: R-CACHE-007
- **Discovery**: Plan-B triage
- **Patch**: `398d65d` (double-check after re-lock; release duplicate handle if another goroutine resolved first)
- **Regression test**: T-I-015 (pending)
- **Verified red-green**: pending hardware

### B-004 parseSymbol discards errors silently
- **Severity**: LOW
- **Spec basis**: R-CACHE-001
- **Discovery**: Plan-B triage
- **Patch**: `398d65d` (added Warn logging on parse failure)
- **Regression test**: synthetic (low priority)
- **Verified red-green**: not applicable (logging-only fix)

### B-005 SumRead truncated response causes misalignment
- **Severity**: HIGH
- **Spec basis**: R-SUM-006
- **Discovery**: Plan-B triage
- **Patch**: `3d5eca7` (cmd_sum.go truncation cascade)
- **Regression test**: T-U-801 (pending)
- **Verified red-green**: pending

### B-006 STRING write missing null terminator on exact-length input
- **Severity**: HIGH
- **Spec basis**: R-PARSE-003
- **Discovery**: Plan-B triage
- **Patch**: `3d5eca7` (clamp to Length-1)
- **Regression test**: T-U-1004 (pending)
- **Verified red-green**: pending

### B-007 commandAddDeviceNotification comment "1ms" should be "100ns"
- **Severity**: LOW (doc only)
- **Patch**: `3d5eca7`
- **Notes**: Cosmetic.

### B-008 Plaintext credential warning missing on WithRoute and AddRemoteRoute
- **Severity**: LOW (security awareness)
- **Spec basis**: R-ROUTE-003
- **Patch**: `3d5eca7` (logger.go + route.go warnings)
- **Regression test**: T-DOC-003 (pending)

### B-009 sumWriteChecked TOCTOU race
- **Severity**: HIGH
- **Spec basis**: R-SUM-002
- **Discovery**: Plan-B Phase 1
- **Patch**: `f5d9dae` "Fix: totalReadLen uint32→uint64, overflow guard works..." + atomic state machine
- **Regression test**: T-U-509 (pending)
- **Verified red-green**: pending

### B-010 totalReadLen overflow on 32-bit Go
- **Severity**: HIGH
- **Spec basis**: R-PARSE-006
- **Patch**: `f5d9dae`
- **Regression test**: T-U-1007 (pending)

### B-011 invokeID echo missing for route registration
- **Severity**: HIGH (security)
- **Spec basis**: R-CMD-008
- **Patch**: route.go random invokeID via crypto/rand
- **Regression test**: T-U-701, T-U-900 (pending)

### B-012 Stale Symbol.Handle after reconnect
- **Severity**: HIGH
- **Spec basis**: R-CACHE-004, R-RECON-005
- **Patch**: connection.go zeroOldSymbolHandles
- **Regression test**: T-U-203 (pending)
- **Verified red-green**: pending hardware

### B-013 First-sample race window: "unknown handle" warn spam
- **Severity**: LOW
- **Spec basis**: R-NOT-007
- **Patch**: lastSubscribeNs atomic + 100ms suppression
- **Regression test**: T-U-108 (pending)

### B-014 Resubscribe partial-commit not rolled back
- **Severity**: HIGH
- **Spec basis**: R-NOT-014
- **Patch**: connection.go resubscribeNotifications rollback path
- **Regression test**: T-I-009 (pending hardware)

(F-numbered bugs B-015..B-031 follow the same pattern; backfill task to enumerate from `git log`.)

---

## Round 2: First multi-agent review

Found AFTER Plan-C squash; these are bugs in Plan-C-introduced code OR pre-existing bugs that surfaced during review.

### B-101 SumNotifState shared between Add (0xF085) and Delete (0xF086)
- **Severity**: CRITICAL
- **Spec basis**: R-SUM-003
- **Discovery**: Round 2, Devil's Advocate agent
- **Patch**: `919d2b2` "fix(capabilities): split sumNotifState into add/delete (review-critical)"
- **Regression test**: T-U-800 (exists in Plan-C squash test additions, pending verification)
- **Verified red-green**: yes — TC2 hardware test (`TestIntegrationReconnect`) shows independent 0x0701 probes for both opcodes post-fix; pre-fix only one probe.

### B-102 Duplicate-symbol subscribe leaks PLC handle + notificationConfigs
- **Severity**: HIGH
- **Spec basis**: R-NOT-002, R-NOT-008
- **Discovery**: Round 2, Errors+Concurrency agent F3
- **Patch**: `fc5ff60` "fix: notification + parse safety hardening (review-high)"
- **Regression test**: T-U-101, T-U-102, T-U-103 (pending)
- **Verified red-green**: pending

### B-103 AddSymbolNotification TOCTOU on shared channel
- **Severity**: HIGH
- **Spec basis**: R-NOT-001, R-NOT-003
- **Discovery**: Round 2, CodeRabbit
- **Patch**: `fc5ff60` (post-PLC re-check, release-handle on conflict)
- **Regression test**: T-U-104 (pending)
- **Verified red-green**: pending

### B-104 zeroOldSymbolHandles only zeroes Handle (incomplete reset)
- **Severity**: HIGH
- **Spec basis**: R-CACHE-004
- **Discovery**: Round 2, Errors+Concurrency
- **Patch**: `fc5ff60` (also clear Value/Valid/ValueParsed/LastUpdateTime)
- **Regression test**: T-U-203 (pending)
- **Verified red-green**: pending

### B-105 parentChanged unbounded recursion (potential stack overflow on cycle)
- **Severity**: HIGH (defense-in-depth)
- **Spec basis**: R-PARSE-002, R-SYM-002
- **Discovery**: Round 2, Devil's Advocate
- **Patch**: `fc5ff60` (iterative walk + 256-depth cap)
- **Regression test**: T-U-1003 (pending)
- **Verified red-green**: pending

### B-106 WSTRING write splits surrogate pairs on truncation
- **Severity**: HIGH
- **Spec basis**: R-PARSE-004
- **Discovery**: Round 2, Devil's Advocate
- **Patch**: `fc5ff60` (drop trailing high surrogate)
- **Regression test**: T-U-1005 — already exists in `review_round4_test.go` as `TestWSTRINGSurrogatePairTruncation`.
- **Verified red-green**: yes

### B-107 NotificationConfig int(ms) silent breaking
- **Severity**: HIGH (silent breaking change)
- **Spec basis**: R-NOT-009 (API consistency)
- **Discovery**: Round 2, multiple agents
- **Patch**: `75dfb28` (int → time.Duration; AddSymbolNotification + NotificationConfig)
- **Regression test**: covered by T-U-110 + T-I-008 with new types.
- **Verified red-green**: yes (compile-time + hardware)

### B-108 SymbolView lazy back-ref orphaned post-reload
- **Severity**: MEDIUM
- **Spec basis**: R-VIEW-001 (pre-snapshot revert)
- **Discovery**: Round 2, Architecture
- **Patch**: `75dfb28` (lazy lookup) → later reverted in Round 3 to snapshot per Round-3 verdict.
- **Regression test**: T-U-400 (pending)
- **Verified red-green**: pending; design changed twice — post-Round-3 is the canonical model.

### B-109 sumCmdSpec.decode unused param
- **Severity**: LOW
- **Spec basis**: R-SUM-007 (cleanup)
- **Patch**: `675ad47` (drop param)
- **Regression test**: not applicable (refactor only)

### B-110 SumDeleteResult single-field wrapper pointless
- **Severity**: LOW
- **Patch**: `75dfb28` (use `[]ReturnCode` directly)
- **Regression test**: not applicable

---

## Round 3: Second multi-agent review

After Round 2 fixes; surfaced subtler concurrency edge cases.

### B-201 Stale-Symbol race window between cache.lock release and notifs.lock acquire
- **Severity**: HIGH
- **Spec basis**: R-NOT-004
- **Discovery**: Round 3, Architecture + Errors agents
- **Patch**: `675ad47` (initial fresh-fetch) + Round 4 generation counter (full close)
- **Regression test**: T-U-105 (pending)
- **Verified red-green**: pending

### B-202 ChildrenWalk holds cache.lock during user fn (deadlock risk)
- **Severity**: HIGH
- **Spec basis**: R-VIEW-004
- **Discovery**: Round 3, multiple agents
- **Patch**: `8451358` (collect-then-iterate) — but Round-3 squash had recursive lock; final fix in Round 4 commit.
- **Regression test**: T-U-403 (pending), T-U-404 (exists in Round 4 WIP)
- **Verified red-green**: pending

### B-203 SymbolView.Value() vs Valid() not atomic
- **Severity**: MEDIUM
- **Spec basis**: R-VIEW-001
- **Discovery**: Round 3, Architecture
- **Patch**: `2d3f6a9` (revert to snapshot fields) + Round 4 reaffirm
- **Regression test**: T-U-400 (pending)

### B-204 SymbolView.Children() N×lock acquisitions
- **Severity**: MEDIUM (perf trap)
- **Spec basis**: R-VIEW-003
- **Discovery**: Round 3, Devil's Advocate + Architecture
- **Patch**: `2d3f6a9` (revert to snapshot)
- **Regression test**: T-U-402 (pending; should benchmark large symbol walk)

### B-205 resubscribeNotifications drops Skipped+Handle entries
- **Severity**: MEDIUM
- **Spec basis**: R-NOT-014, R-NOT-015
- **Discovery**: Round 3, Errors+Concurrency
- **Patch**: `2d3f6a9` (orphan handles released via bestEffortDelete)
- **Regression test**: T-I-009 covers; T-U-115/T-U-116 (pending) for retry counter.
- **Verified red-green**: pending

### B-206 routeProbeFailures int → atomic
- **Severity**: LOW
- **Spec basis**: R-LOCK-003
- **Discovery**: Round 3, Errors+Concurrency
- **Patch**: `75dfb28` (atomic.Int32 migration)
- **Regression test**: T-U-1100 (pending)

### B-207 onDisconnect callback after Close window
- **Severity**: LOW
- **Spec basis**: R-SES-007 (documented contract)
- **Discovery**: Round 3, Errors+Concurrency
- **Patch**: deferred — accepted as documented contract.
- **Regression test**: not applicable (documented behavior)

---

## Round 4: Third multi-agent review

After Round 3 squash; final pre-spec review surfaced these.

### B-301 handleNotification used stranded Symbol pointer
- **Severity**: HIGH
- **Spec basis**: R-NOT-005
- **Discovery**: Round 4, Errors+Concurrency
- **Patch**: WIP `31ac1c9` (re-resolve via cache.symbols[FullName] under cache.lock)
- **Regression test**: T-U-106 (pending)
- **Verified red-green**: pending; will validate during Round-4 fix merge.

### B-302 cache.generation residual race (third loadSymbols mid-window)
- **Severity**: HIGH
- **Spec basis**: R-NOT-004
- **Discovery**: Round 4, Architecture
- **Patch**: WIP `31ac1c9` (capture+recheck via atomic.Load under notifs.lock)
- **Regression test**: T-U-105 (pending)
- **Verified red-green**: pending

### B-303 Unbounded handleReceive goroutines (`go conn.handleReceive` per packet)
- **Severity**: HIGH
- **Spec basis**: R-TX-006 (worker pool requirement, NEW)
- **Discovery**: Round 4, Devil's Advocate (LB-1)
- **Patch**: WIP `31ac1c9` (recvQueue + 16 worker goroutines)
- **Regression test**: T-U-507 (pending), T-I-020 aspirational
- **Verified red-green**: pending

### B-304 conn.source.NetID lock-free write in Connect
- **Severity**: MEDIUM
- **Spec basis**: R-LOCK-005, R-SES-002
- **Discovery**: Round 4, Devil's Advocate (LB-2)
- **Patch**: WIP `31ac1c9` (connMu around source.NetID writes)
- **Regression test**: not directly testable (defensive); covered by T-U-509 race-detector run.

### B-305 addOffset recursion has no depth cap
- **Severity**: MEDIUM (defense-in-depth)
- **Spec basis**: R-SYM-002
- **Discovery**: Round 4, CodeRabbit
- **Patch**: WIP `31ac1c9` (256-depth cap)
- **Regression test**: T-U-300 (exists in WIP as `TestAddOffsetDepthCap`)
- **Verified red-green**: pending

### B-306 resubscribeNotifications Skipped configs not retried on next reconnect
- **Severity**: MEDIUM
- **Spec basis**: R-NOT-013
- **Discovery**: Round 4, CodeRabbit
- **Patch**: WIP `31ac1c9` (re-append Skipped + retry counter, max 3 attempts)
- **Regression test**: T-U-115, T-U-116 (pending)
- **Verified red-green**: pending

### B-307 4 MB AMS-TCP allocation per packet (combined with B-303 → RSS pressure)
- **Severity**: LOW
- **Spec basis**: R-TX-003 (already enforced)
- **Discovery**: Round 4, Devil's Advocate (LB-3)
- **Patch**: B-303 (worker pool) bounds concurrent allocations to recvWorkerCount × 4MB = 64MB max in flight.
- **Regression test**: not separately required.

### B-308 cache.generation doc unclear about insert vs swap
- **Severity**: LOW
- **Spec basis**: R-CACHE-003 (clarified in spec)
- **Discovery**: Round 4, CodeRabbit
- **Patch**: WIP `31ac1c9` (doc clarification)
- **Regression test**: T-DOC-001 (pending)

### B-309 "UTnull" comment typo
- **Severity**: LOW (cosmetic)
- **Patch**: WIP `31ac1c9` ("UTF-16 null")

### B-310 parentChanged double-marks self.Changed
- **Severity**: LOW (cosmetic)
- **Spec basis**: R-PARSE-002
- **Patch**: WIP `31ac1c9` (start at Parent)
- **Regression test**: T-U-1002 (test exists in `ads_test.go`, updated for new contract)

### B-311 SymbolView.Valid field name vs IsValid() method semantic clash
- **Severity**: LOW (API ergonomics)
- **Patch**: WIP `31ac1c9` (rename field → `Parsed`)
- **Regression test**: not separately required (covered by T-U-400, T-U-401).

---

## Open / Deferred (not bugs, but tracked)

These are findings from Round 4 that were rejected as overengineered or deferred.

- **D-001** Devil's Advocate Round 4: cache.generation pattern overengineered. Rejected — validation agents confirmed correctness; required to satisfy R-NOT-004.
- **D-002** Devil's Advocate Round 4: handleNotification re-resolve overengineered. Rejected — required to satisfy R-NOT-005.
- **D-003** Devil's Advocate Round 4: ChildrenWalk hold-lock-for-walk. Accepted — fixed in B-202 (collect-then-iterate).
- **D-004** Round 1 / 4: capabilities Store methods only used by tests. Accepted as-is for test convenience; documented under R-LOCK-003.
- **D-005** Round 4: connection.go grew to 1005 LoC; split into lifecycle.go suggested. Deferred to next architectural campaign.
- **D-006** Round 4: capabilities anemic wrappers around atomic types. Deferred — not blocking, low risk.
- **D-007** Round 4: extract `(c *symbolCache).resolveAndCaptureGen()` helper for the 5 duplicate sites. Deferred — defensible duplication for now.
- **D-008** Round 4: transport.go anemic, no methods. Deferred to architectural campaign alongside D-005.

---

## Backfill task

For the 30+ bugs above, only B-101 has confirmed red-green status (via TC2 hardware behavior). The rest need:

1. Synthetic reproduction test under `T-U-NNN` ID.
2. Confirmation that the test fails on the parent commit of the fix.
3. Confirmation that the test passes on the fix commit.
4. Update of `Verification:` field in `01-requirements.md`.
5. Update of `Verified red-green:` field here.

This backfill is a prerequisite for v1.0 release. Estimated effort: 30 bugs × ~30 min per regression test = ~15 hours.

## How to file a new bug

1. Add `B-NNN` entry following the template above.
2. Identify the `R-XXX-NNN` violated (or file new requirement if behavior was unspecified).
3. Reproduce on unpatched code.
4. Apply patch.
5. Verify red-green per `06-tdd-protocol.md`.
6. Update this document + the requirement's `Verification:` field.
7. Cite spec ID in commit message (`fix(R-NOT-005): re-resolve symbol via FullName under cache.lock`).
