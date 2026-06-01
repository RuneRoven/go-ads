# go-ads Spec ↔ Code Audit (2026-05-10)

Three-axis audit of `specs/01-requirements.md` against production code (post Phase 5 / DP-2 landed).

Scope:
- Axis 1: spec → code drift (does code fulfil each R-XXX-NNN?).
- Axis 2: NO-SPEC tests → recommend backfill / keep / delete.
- Axis 3: spec quality (stale references, ambiguity, contradictions).

DP-1 / DP-3 implementation is pending (task #33). The following are KNOWN-MISSING and explicitly NOT flagged in axis 1: `R-CACHE-009..018`, `R-NOT-016`, `R-NOT-017`, `R-SES-011`. They are still flagged in axis 3 where their wording is broken regardless of implementation.

R-CON-* prefix was renamed R-SES-* on 2026-05-08. Treated as renamed.

---

## Axis 1 — Spec → Code drift

Only MISSING / PARTIAL / DRIFT entries listed. Anything fully fulfilled in production code is omitted.

### Module SES — Session lifecycle

| Entry | Type | Sev | Finding | Action |
|---|---|---|---|---|
| R-SES-001 | DRIFT | HIGH | Spec calls constructor `NewConnection(...)` returning `*Connection`. Code is `NewSession(...)` returning `*Session` (`session.go:130`). Phase 5 rename. | Rewrite spec wording: NewSession / *Session. |
| R-SES-005 | DRIFT | MEDIUM | Spec text: "fresh `NewConnection`". Closed Session is rejected via `lifecycle.state == Closed` (FSM, not `closed.Load()`). `session_fsm.go:191`. | Rewrite spec to reference FSM-based gate; rename to NewSession. |
| R-SES-008 | DRIFT | MEDIUM | Spec invariant says "`lifecycle.disconnected.CompareAndSwap(false, true)` gates callback dispatch". Real gate is `tx.disconnected.CompareAndSwap` (`session.go:528`); also FSM transitions to Disconnected only on first detector. | Rewrite invariant to `sess.tx.disconnected.CompareAndSwap` + Disconnected transition. |
| R-SES-011 | KNOWN MISSING | — | DP-1 strategies, `WithOnSymbolVersionChanged`. Task #33. Not flagged. | — |

### Module CL — Client (raw RPC)

All eight R-CL-* entries are fulfilled in code. No drift.

### Module NOT — Notifications

| Entry | Type | Sev | Finding | Action |
|---|---|---|---|---|
| R-NOT-001 | DRIFT | LOW | "All `AddSymbolNotification` calls on a single Connection" — no Connection type any more. | Rename Connection → Session. |
| R-NOT-004 | DRIFT | HIGH | Spec title: "Stranded-Symbol race defense via cache.generation". `cache.generation` was folded into `sessionFSM.epoch` (`session_fsm.go:97`). Code uses `sess.epoch()` (`notification_api.go:120-145, 298-318`). Behaviour preserved; spec wording obsolete. | Rewrite: epoch instead of cache.generation. |
| R-NOT-005 | DRIFT | LOW | Spec mentions "stranded pointer from `notifs.activeNotifications`" — still accurate, but mentions cache.generation indirectly via R-NOT-004. | Update cross-ref. |
| R-NOT-016 | KNOWN MISSING | — | DP-1 fields. Task #33. Not flagged. | — |
| R-NOT-017 | KNOWN MISSING | — | DP-1 dispatch. Task #33. Not flagged. | — |

### Module CACHE — Symbol cache

| Entry | Type | Sev | Finding | Action |
|---|---|---|---|---|
| R-CACHE-003 | DRIFT | HIGH | Title and body refer to "generation increments on swap". Field renamed: `cache.generation` removed; `epoch` (on `sessionFSM`) is the unified counter (`session_fsm.go:94-105`). `bumpEpoch()` is the swap-time helper (`session_fsm.go:217-224`). | Rewrite: swap calls `bumpEpoch()`; epoch atomic.Uint64. |
| R-CACHE-009..018 | KNOWN MISSING | — | DP-1 / DP-3 not implemented. Task #33. Not flagged in axis 1. | — |

### Module SYM — Symbol type

| Entry | Type | Sev | Finding | Action |
|---|---|---|---|---|
| R-SYM-002 | DRIFT | HIGH | Spec invariant: `addOffsetMaxDepth = parentChangedMaxDepth = collectSubtreeMaxDepth = 256`. `parentChanged` removed in commit `489834c` (DP-2 LANDED 2026-05-08). `parentChangedMaxDepth` const no longer exists. | Drop parentChangedMaxDepth from invariant. |

### Module VIEW — SymbolView

All R-VIEW-* entries match code. No drift. (R-VIEW-005 enforced at `symbol_discovery.go: ListSymbols`.)

### Module TX — Transport

| Entry | Type | Sev | Finding | Action |
|---|---|---|---|---|
| R-TX-001 | DRIFT | LOW | Title: "Single TCP connection per Connection". | Rename Connection → Session (Client owns transport now post-Phase 5). |
| R-TX-005 | DRIFT | MEDIUM | Statement: `lifecycle.reconnecting==true` check. There is no `lifecycle.reconnecting` boolean any more — readiness is `state == SessionStateReconnecting` via `isReconnecting()`. Behaviour preserved; spec wording obsolete. | Rewrite: FSM `isReconnecting()` check; sendRequest now lives on `*Client` with Session-side wrapper providing retry semantics — split if accuracy desired. |

### Module RECON — Reconnect FSM

| Entry | Type | Sev | Finding | Action |
|---|---|---|---|---|
| R-RECON-002 | DRIFT | HIGH | Spec: "CAS `lifecycle.disconnected` from false to true" + "`lifecycle.reconnecting` (atomic.Bool) is CAS'd". Real code: `tx.disconnected.CompareAndSwap` + FSM `transitionToOnce(SessionStateReconnecting)` (no `lifecycle.reconnecting` atomic.Bool exists). `session.go:528`, `session_fsm.go:148-181`. | Rewrite: tx.disconnected + FSM transitionToOnce. |
| R-RECON-005 | DRIFT | HIGH | Title: "reconnectGeneration counter". Field removed; folded into `sessionFSM.epoch` (`session_fsm.go:97`). Retry helpers do exist (`resetForRetry`, `session.go:1036`) but use FSM/epoch, not a separate counter. | Rewrite: epoch is the stale-handle detector. |
| R-RECON-008 | DRIFT | MEDIUM | Spec mentions `lifecycle.closed.Load()` — closed flag removed (FSM-only since Phase 3.b). `dialAndStart` now uses `sess.isClosed()` (`session.go:672`). | Rewrite: isClosed(). |
| R-RECON-010 | DRIFT | LOW | Spec: "set `lifecycle.disconnected = false`". Real field: `tx.disconnected.Store(false)` (`session.go:672, 879`). | Rename field. |

### Module CMD

All R-CMD-* fulfilled.

### Module SUM

All R-SUM-* fulfilled. R-SUM-008 explicitly anchors handle-preference behaviour matched by `symbolSumAddress` callsites.

### Module ROUTE

All R-ROUTE-* fulfilled.

### Module PARSE

| Entry | Type | Sev | Finding | Action |
|---|---|---|---|---|
| R-PARSE-002 | DELETED IN CODE | HIGH | `parentChanged` walk + `Symbol.Changed` field + `onlyChanged` parameter were removed (DP-2 LANDED 2026-05-08). Spec entry still describes a function that does not exist. | DELETE R-PARSE-002 from spec. Also strike its reference in R-SYM-002 invariant. |

### Module LOCK

All R-LOCK-* fulfilled (R-LOCK-001 just reiterates R-CACHE-008).

---

### Axis 1 summary

- HIGH severity drift: 6 (R-NOT-004, R-CACHE-003, R-SYM-002, R-RECON-002, R-RECON-005, R-PARSE-002)
- MEDIUM: 5 (R-SES-005, R-SES-008, R-TX-005, R-RECON-008, R-SES-001)
- LOW: 4 (R-NOT-001, R-NOT-005, R-TX-001, R-RECON-010)
- KNOWN-MISSING (excluded): R-SES-011, R-NOT-016, R-NOT-017, R-CACHE-009..018

Pattern: every drift is the result of two refactors that the spec was not updated for — Phase 5 (`Connection` → `Client`+`Session`) and DP-2 (`Symbol.Changed`/`parentChanged` removal). Behaviour in production is correct; spec wording is stale.

---

## Axis 2 — NO-SPEC tests → recommend backfill / keep / delete

51 NO-SPEC tests across 4 files. Grouped by topic.

### Group A — Defs / wire-protocol formatters (`defs_test.go`, 10 tests)

| Test | File:line | Recommendation |
|---|---|---|
| TestStringToNetID | defs_test.go:13 | BACKFILL as **R-PARSE-008 — NetID string parsing**. Used by `WithRoute`, public-ish helper. |
| TestStringToNetIDErrors | defs_test.go:40 | Same — fold into R-PARSE-008. |
| TestTransModeString | defs_test.go:91 | KEEP as regression-guard. Stringer for log output; not load-bearing for behaviour. |
| TestSymbolFlagHas | defs_test.go:129 | BACKFILL as **R-SYM-009 — SymbolFlag bit-test helper**. Required by ContextMask logic (R-SYM-005). |
| TestSymbolFlagBitValue_Detection | defs_test.go:144 | Same — fold into R-SYM-009. |
| TestReturnCodeString | defs_test.go:164 | KEEP as regression-guard (Stringer). |
| TestReturnCodeError | defs_test.go:188 | BACKFILL as **R-CMD-009 — ReturnCodeError implements error**. Caller-facing error wrapping. |
| TestReturnCodeString_AllCategories | defs_test.go:199 | DELETE — coverage-theater per existing audit (10-test-audit.md A10). Re-asserts the switch table. |
| TestAppendNull | defs_test.go:290 | KEEP as regression-guard for route packet building. |
| TestProcessImageConstants | defs_test.go:313 | DELETE — coverage theater. Re-asserts numeric constants (10-test-audit.md A3). |

### Group B — Process-image bit ops (`symbol_codec_test.go`, 5 tests)

| Test | File:line | Recommendation |
|---|---|---|
| TestReadBit_Extract | symbol_codec_test.go:1172 | BACKFILL as **R-PARSE-009 — Bit-addressed read/write helpers**. Used by BOOL-in-byte process-image access. |
| TestReadBit_AllPositions | :1187 | Fold into R-PARSE-009. |
| TestWriteBit_Set | :1202 | Fold into R-PARSE-009. |
| TestWriteBit_Clear | :1211 | Fold into R-PARSE-009. |
| TestWriteBit_PreservesOthers | :1220 | Fold into R-PARSE-009 (HIGH — write must not clobber adjacent bits). |

### Group C — STRING/WSTRING / Codec edge cases (`symbol_codec_test.go`, 11 tests)

| Test | File:line | Recommendation |
|---|---|---|
| TestWriteToNode_STRING_RejectsZeroLength | :52 | BACKFILL — extend R-PARSE-003 invariant: "STRING write requires Length >= 1". |
| TestWriteToNode_WSTRING_RejectsTooShort | :67 | BACKFILL — extend R-PARSE-004: "WSTRING write requires Length >= 2". |
| TestWriteToNode_FallsBackToInferredType | :87 | BACKFILL as **R-PARSE-010 — WriteToNode type-inference fallback** (when DataType is unknown but Length matches a primitive size). |
| TestWriteToNode_RejectsUnknownTypeWithUninferableSize | :110 | Fold into R-PARSE-010 (negative complement). |
| TestSymbolParseUnknownType | :195 | Fold into R-PARSE-010 (parse-side complement). |
| TestWriteToNodeAliasResolution | :368 | BACKFILL as **R-PARSE-011 — Type alias resolution via datatypes map**. |
| TestWriteToNodeAliasWithoutDatatypes | :387 | Fold into R-PARSE-011 (negative path). |
| TestWriteToNodeUnknownType | :396 | Fold into R-PARSE-010. |
| TestSymbolParseAliasResolution | :450 | Fold into R-PARSE-011. |
| TestWriteToNodeInvalidValues | :472 | KEEP as regression-guard. Negative coverage, no clean R-XXX hook. |
| TestWriteToNodeStructInvalidJSON | :506 | KEEP as regression-guard. |

### Group D — Symbol tree builders / array helpers (`symbols_test.go`, 19 tests)

| Test | File:line | Recommendation |
|---|---|---|
| TestMakeArrayChildren_HappyPath | :45 | BACKFILL as **R-SYM-010 — Array children construction**. Spec is silent on the array-flatten algorithm. |
| TestMakeArrayChildren | :434 | Fold into R-SYM-010. |
| TestMakeArrayChildrenEmpty | :458 | Fold (zero-element case). |
| TestMakeArrayChildrenNonZeroLBound | :467 | Fold (LBound!=0 case — IEC 61131-3). |
| TestMakeArrayChildren_2D | :480 | Fold (multi-dim). |
| TestMakeArrayChildren_3D | :507 | Fold. |
| TestMakeArrayChildren_ZeroElements | :537 | Fold. |
| TestParseUploadSymbolInfoDataTypes_Empty | :63 | KEEP — guards empty-buffer no-panic. |
| TestParseUploadSymbolInfoSymbols_Empty | :77 | KEEP — same. |
| TestAddChildren | :400 | BACKFILL as **R-SYM-011 — addChildren utility no-overwrite invariant** (test #2 below proves the contract). |
| TestAddChildrenNoDuplicates | :415 | Fold into R-SYM-011. |
| TestInferBaseType | :549 | BACKFILL as part of R-SYM-004 invariants — explicitly the "size→type fallback" path. Currently invisible in spec. |
| TestGetJSON | :574 | KEEP — Stringer-style. |
| TestGetJSONBool | :584 | KEEP. |
| TestGetJSONString | :594 | KEEP. |
| TestGetJSONStruct | :604 | KEEP. |
| TestGetJSON_EmptyValue | :618 | KEEP. |
| TestGetJSON_NumericOverflow | :629 | DELETE — coverage theater (10-test-audit.md A6). Asserts only `j != ""` for ULINT max. |
| TestGetJSON_WSTRINGAsString | :647 | KEEP. |

### Group E — symbolSumAddress (`symbols_test.go`, 5 tests)

All five anchor to the existing **R-SUM-008** entry. Test audit (10-test-audit.md) flagged "should anchor in a new R-SUM (handle preference)" — that anchor already exists as R-SUM-008. The `// Validates: NO-SPEC` comments are stale; just relabel.

| Test | File:line | Recommendation |
|---|---|---|
| TestSymbolSumAddress_PrefersHandleOverDirect | :742 | RELABEL Validates: R-SUM-008 (no spec change needed). |
| TestSymbolSumAddress_HandleOnlyNoGroup | :761 | Same. |
| TestSymbolSumAddress_DirectFallbackNoHandle | :779 | Same. |
| TestSymbolSumAddress_DirectFallbackChildAccumulatesOffset | :797 | Same. |
| TestSymbolSumAddress_DirectFallbackNestedChild | :822 | Same. |

### Group F — Notification packet edge case (`cmd_notification_test.go`, 1 test)

| Test | File:line | Recommendation |
|---|---|---|
| TestDeviceNotification_ZeroStamps | cmd_notification_test.go:278 | KEEP as regression-guard. Empty-stamps degenerate case; no clean R-XXX anchor needed. |

---

### Axis 2 summary

- **Backfill** (new R-XXX-NNN entries proposed): 7 new IDs, ~25 tests reattached.
  - R-PARSE-008 (NetID parsing), R-PARSE-009 (bit ops), R-PARSE-010 (type inference), R-PARSE-011 (alias resolution)
  - R-SYM-009 (Flag bit helpers), R-SYM-010 (array children), R-SYM-011 (addChildren)
  - R-CMD-009 (ReturnCodeError)
- **Relabel only** (Validates: tag wrong, spec already covers): 5 (Group E → R-SUM-008).
- **Keep as regression-guard** (no spec needed; defensive negative-path coverage): 14.
- **Delete** (coverage theater, already flagged in 10-test-audit.md): 3 — `TestReturnCodeString_AllCategories`, `TestProcessImageConstants`, `TestGetJSON_NumericOverflow`. (Existing audit also flags `TestEncodePacket_AllCommands` — already non-NO-SPEC.)
- Two STRING/WSTRING tests get folded as additional invariants on R-PARSE-003/004 (no new ID needed).

---

## Axis 3 — Spec quality

124 R-XXX-NNN entries. Defects grouped.

### A. Stale code references (highest priority — auto-rewrite)

| Spec entry | Defect | Recommendation |
|---|---|---|
| R-SES-001 | "Connection construction is total" / "*Connection". | Rewrite: NewSession / *Session. |
| R-SES-005 | "fresh `NewConnection`". | Rename. |
| R-SES-008 | invariant cites `lifecycle.disconnected.CompareAndSwap`. | Replace with `tx.disconnected.CompareAndSwap` + FSM transition. |
| R-NOT-001 | "on a single Connection". | Rename. |
| R-NOT-004 | title: "via cache.generation"; statement uses cache.generation snapshot. | Rewrite around `sess.epoch()`. |
| R-CACHE-003 | "generation increments on swap" — `cache.generation` field gone, replaced by `epoch` + `bumpEpoch()`. | Rewrite. |
| R-CACHE-018 | "Generation-based view staleness" — references the dead counter name; DP-3 entry already pending. | Rewrite or merge into R-CACHE-014. |
| R-SYM-002 | invariant lists `parentChangedMaxDepth`. | Drop that token. |
| R-PARSE-002 | entire entry describes deleted function `parentChanged()` and field `Symbol.Changed`. | DELETE entry. |
| R-VIEW-004 | invariant: "256-depth cap matching `addOffsetMaxDepth` and `parentChangedMaxDepth`". | Drop parentChanged token. |
| R-TX-001 | "Single TCP per Connection". | Rename. |
| R-TX-005 | "if `lifecycle.reconnecting==true`". No such field. | Rewrite as `isReconnecting()`. Also note sendRequest moved to Client. |
| R-RECON-002 | Statement: "CAS `lifecycle.disconnected`" + "`lifecycle.reconnecting` (atomic.Bool)". Both wrong now. | Rewrite: `tx.disconnected.CompareAndSwap` + FSM `transitionToOnce(SessionStateReconnecting)`. |
| R-RECON-005 | Title: "reconnectGeneration counter". Field gone, folded to `epoch`. | Rewrite around epoch. |
| R-RECON-008 | "re-check `lifecycle.closed.Load()`". Field gone. | Rewrite: `sess.isClosed()`. |
| R-RECON-010 | "set `lifecycle.disconnected = false`". | Replace with `tx.disconnected.Store(false)`. |
| R-LOCK-003 | mentions "cache.generation" in invariants. | Replace with epoch. |

### B. Ambiguous / wishy-washy SHALL language

| Spec entry | Defect | Recommendation |
|---|---|---|
| R-SES-005 | "Re-use SHALL be rejected (`Connect` returns error or panics — current code uses error path…)". The "or panics" weakens the contract. | Pick one: error path. Drop "or panics". |
| R-SES-007 | "The library does NOT enforce this; documented in godoc." Says nothing testable. | Either drop the SHALL or add a doc-content T-DOC test. |
| R-RECON-010 | invariant: "small window where IsDisconnected returns false but listen/transmit not yet running; callers retry on stale handles". This describes a *bug accepted as feature*. | Either elevate to known-issue annex or remove. |
| R-TX-005 | "retries once on context.Canceled IF a reconnect is in progress AND the connection isn't permanently closed". Three conjuncted predicates without test anchor. | Add T-U test IDs that pin each predicate. |
| R-NOT-007 | "MUST NOT generate a Warn-level… SHALL track lastSubscribeNs… downgrade the log to Debug for ~100ms". The "~100ms" is fuzzy; constant is `100*time.Millisecond` exactly. | Replace ~100ms with the named constant `subscribeRaceWindowNs`. |

### C. Defends a fantasy scenario (no usage-pattern trace)

| Spec entry | Defect | Recommendation |
|---|---|---|
| R-NFR-002 | "Hardware tests required for release" — non-functional, audit-only; not a behavioural requirement of the library. | Move to `02-quality-constitution.md` or a CI policy doc; remove from `01-requirements.md`. |
| R-NFR-003 | "Linter clean" — same. | Move out. |
| R-NFR-004 | "Deterministic unit tests" — same. | Move out. |
| R-NFR-001 | "Backwards-incompatible changes need version bump" — release process, not library behaviour. | Move out. |

### D. References deleted/renamed code

(Already covered in section A — top of axis 3.)

### E. Internal contradictions

| Spec entry | Defect | Recommendation |
|---|---|---|
| R-CACHE-008 + R-LOCK-001 | Same statement duplicated; LOCK module just reiterates. | Merge — keep R-CACHE-008 only; have R-LOCK-001 as a one-liner pointer. (Already mostly so; verify cross-ref renders.) |
| R-NOT-004 + R-CACHE-003 + R-CACHE-018 + R-RECON-005 | All four reference the same `cache.generation` / `reconnectGeneration` mechanism that has been folded into `epoch`. Each entry paraphrases the others. | Consolidate: have a single canonical "Epoch counter for stranded-pointer detection" entry; the others reference it. |
| R-SES-002 + R-ROUTE-004 | R-SES-002 says "optionally register an AMS route via UDP". R-ROUTE-004 prescribes the probe-then-register protocol. The "optionally" weakens R-ROUTE-004's algorithm. | Tighten R-SES-002 to point at R-ROUTE-004 for the registration sub-procedure. |
| R-CACHE-014 vs R-CACHE-009..013 | R-CACHE-014 ("cache state machine") is the union of the strategy entries. With DP-1 unimplemented, R-CACHE-014 effectively duplicates 009..013. | Merge — make R-CACHE-014 the parent and 009..013 sub-cases under it; drop 014's standalone entry. |

### F. Verifies-field references that don't exist

The spec uses informal `T-U-XXX` / `T-I-XXX` test IDs throughout the Verification field. Cross-ref to `10-test-audit.md` Section 3 shows many of these IDs were never created (e.g. `T-U-205`, `T-U-1003`, `T-U-1100`, `T-U-CL-008`). These aren't outright errors but render the spec untrustworthy as a traceability artifact.

| Defect | Severity | Recommendation |
|---|---|---|
| Verification field cites synthetic test IDs (`T-U-NNN`) that no test in the repo carries via `// Validates:` comment. | MEDIUM | Either: (a) add the IDs to actual tests as `// Validates: R-XXX-NNN [T-U-NNN]`, or (b) drop the T-U-NNN bookkeeping and rely solely on `// Validates: R-XXX-NNN`. The current state is the worst of both. |
| R-PARSE-002 cites T-U-1002 / T-U-1003 — function and tests both deleted. | HIGH | Delete entry. |

### G. Jargon a new contributor wouldn't understand

| Spec entry | Issue | Recommendation |
|---|---|---|
| R-NOT-004 | "Stranded-Symbol race defense" — undefined elsewhere in spec. | Add a one-line glossary entry or expand on first use ("a `*Symbol` pointer captured before a cache swap; subsequent reads through it would touch the old map"). |
| R-NOT-007 | "first-sample race window" — same. | Glossary or expand on first use. |
| R-NOT-011 | "in-context transmission mode auto-fallback" — opaque without TwinCAT TransMode background. | Reference Beckhoff PROTOCOL.md or define Server/Client/InContext. |
| R-CACHE-016 | "(gen, ptr) tuple" — never defined elsewhere. | Either define or drop the tuple language; the behaviour is "view re-resolves via FullName". |
| R-VIEW-004 | "collect-then-iterate" — readable but undefined. Make explicit: "snapshot all matching `*Symbol`s into a slice under cache.lock, release, then call user iterator". | Already mostly there; rephrase. |

### Axis 3 summary by category

| Category | Count |
|---|---|
| A — Stale code references | 17 |
| B — Ambiguous SHALL | 5 |
| C — Fantasy scenario / non-functional | 4 (R-NFR-001..004) |
| D — Deleted/renamed code (subset of A) | (counted in A) |
| E — Internal contradictions / duplication | 4 |
| F — Verification IDs that don't exist | many — recommended bulk policy change |
| G — Jargon | 5 |

---

## Prioritized action plan

Ordered: HIGH first (production drift / spec contradicts code), then MEDIUM (ambiguity, missing test anchors), then LOW (cleanup).

### HIGH

1. **DELETE R-PARSE-002.** parentChanged removed in commit 489834c (DP-2). Spec describes dead code.
2. **Rewrite R-RECON-002.** Statement is wrong on two fields: `lifecycle.disconnected` and `lifecycle.reconnecting` don't exist. Real gates: `tx.disconnected.CompareAndSwap` + `state.transitionToOnce(SessionStateReconnecting)`.
3. **Rewrite R-RECON-005.** `reconnectGeneration` field gone — folded to `sessionFSM.epoch`. Re-anchor on epoch.
4. **Rewrite R-NOT-004.** Title and body cite `cache.generation`. Real counter is `sess.epoch()`. Behaviour is preserved at `notification_api.go:120-145` — wording is the only defect.
5. **Rewrite R-CACHE-003.** "generation increments on swap" — rename to `bumpEpoch()` invocation (under cache.lock at every swap site: `session.go:789, 1116`, `symbol_discovery.go:142, 550, 602`).
6. **Rewrite R-SYM-002 invariant.** Drop `parentChangedMaxDepth` token. Remaining caps are `addOffsetMaxDepth = collectSubtreeMaxDepth = 256`.
7. **Update R-VIEW-004 invariant** — same `parentChangedMaxDepth` token to drop.
8. **Rename Connection → Session** across all spec entries (`R-SES-001/002/004/005`, `R-NOT-001`, `R-TX-001`, etc.). Mechanical sed pass + review.
9. **Consolidate R-CACHE-014 vs R-CACHE-009..013.** Make 014 the parent; 009..013 sub-cases. Eliminates duplication that will worsen on DP-1 implementation.
10. **Add backfill spec entries** for the 7 proposed IDs (R-PARSE-008..011, R-SYM-009..011, R-CMD-009). 25 NO-SPEC tests get anchored, removing axis-2 ambiguity.

### MEDIUM

11. **Rewrite R-SES-008 invariant** to use `tx.disconnected.CompareAndSwap` instead of `lifecycle.disconnected.CompareAndSwap`.
12. **Rewrite R-RECON-008** to use `sess.isClosed()` (FSM) instead of `lifecycle.closed.Load()`.
13. **Rewrite R-TX-005** to use `isReconnecting()` predicate; clarify that `Client.sendRequest` is the post-Phase-5 location with Session wrapper providing retry semantics.
14. **Rewrite R-LOCK-003** invariant — replace `cache.generation` with `epoch`.
15. **Tighten R-SES-005** — drop "or panics"; pick the error path explicitly.
16. **Tighten R-SES-007** — either remove the SHALL or add a doc-content test.
17. **Tighten R-NOT-007** — replace "~100ms" with the named constant `subscribeRaceWindowNs`.
18. **Move R-NFR-001..004 out of `01-requirements.md`** into `02-quality-constitution.md` or a CI policy doc. They are not behavioural requirements.
19. **Relabel symbolSumAddress tests** (Group E, 5 tests) from `// Validates: NO-SPEC` to `// Validates: R-SUM-008`. No spec change, only test-comment edit.
20. **Audit `Verification:` test IDs.** Either populate every cited `T-U-NNN` as a real test comment, or remove the T-U-NNN field and rely on `// Validates: R-XXX-NNN`.

### LOW

21. **R-NOT-001 / R-NOT-005** — minor Connection→Session rewording.
22. **R-RECON-010** — `lifecycle.disconnected` → `tx.disconnected` rename.
23. **Glossary** for jargon (Stranded-Symbol, first-sample race window, in-context transmission, (gen, ptr) tuple).
24. **DELETE three coverage-theater tests** flagged by 10-test-audit.md and confirmed here: `TestReturnCodeString_AllCategories`, `TestProcessImageConstants`, `TestGetJSON_NumericOverflow`.
25. **Cross-ref pass** R-CACHE-008 ↔ R-LOCK-001 ↔ R-RECON-002 to remove repeated wording — reduce duplication-rot.

---

### One-line summary

axis-1: HIGH=6 MEDIUM=5 LOW=4 (15 total drift, all wording-stale; behaviour correct) | axis-2: backfill=7-new-IDs/25-tests, relabel=5, keep=14, delete=3 (51 NO-SPEC total) | axis-3: stale-refs=17, ambiguous=5, NFR-misplaced=4, contradictions=4, jargon=5
