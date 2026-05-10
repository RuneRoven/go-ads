# Real-World Usage Patterns and PLC Constraints

This document grounds requirements in concrete user behavior and real PLC operating constraints. Without this grounding, requirements drift toward defending fantasy scenarios.

**Rule**: every defensive code path SHALL trace to a concrete usage scenario in this document. If a requirement defends against a scenario not listed here, either add the scenario (after investigation) or remove the defense.

**Rule**: when a behavior is unknown, mark `INVESTIGATION-NEEDED` and stop. Do not assume.

---

## How the library is actually used

### Primary consumer: benthos-umh `beckhoff_ads_plugin`

Source: `/Users/daniel/github/benthos-umh/beckhoff_ads_plugin/ads_input.go`. Verified by reading the plugin code on 2026-05-08.

**Lifecycle**:

1. Plugin starts. `NewConnection` (no I/O).
2. `Connect(false)` — TCP dial + handshake + optional route.
3. **Once**: build `[]NotificationConfig` from user config (typically 1-100 symbols).
4. `AddSymbolNotifications(configs, ch)` — batch subscribe.
5. **Steady state**: loop reading from notification channel, batching messages, ack to benthos.
6. On disconnect (TCP error): library auto-reconnects (default `WithAutoReconnect(true)`).
7. Plugin shutdown: `Close()`.

OR for `readType: "interval"`:

1-2 same as above.
3. Skip notification setup.
4. **Steady state**: periodic `ReadMultipleSymbols(names)` followed by `ReadFromSymbol` fallback for any symbol that errored.
5. On disconnect: auto-reconnect.
6. Plugin shutdown: `Close()`.

**What benthos-umh does NOT do**:

- Never calls `LoadSymbols` / `LoadSymbolsSlow` / `LoadSymbolList` / `LoadDataTypes` (cache is populated lazily via `getSymbol` inside `AddSymbolNotifications` and `ReadMultipleSymbols`).
- Never calls `RefreshSymbols`.
- Never calls `Reconnect` manually (auto-reconnect default).
- Never reuses the same Connection across config reloads — `Close()` then `NewConnection`.

### Other potential consumers

`/Users/daniel/github/go-ads/examples/simple/main.go` — interactive REPL for development. Not production.

**INVESTIGATION-NEEDED**: are there OTHER production consumers (internal or external) we should validate against? If the answer is "only benthos-umh and the example", the spec can be tighter.

### Realistic Connection lifetime

A benthos pipeline runs for hours, days, weeks. The Connection survives:

- Network blips → Reconnect (TCP keepalive Idle=3s detects within ~13s).
- PLC restart → TCP closes → reconnect after PLC comes back up.
- Route registration loss (rare) → re-register on probe failure.

A Connection does NOT survive:

- Process restart.
- benthos config reload that causes the input to be torn down + recreated.

---

## What real PLCs actually do

### TwinCAT 3 runtime

Source: Beckhoff InfoSys + PROTOCOL.md observations.

**Symbol table mutability**:

- After deploy + activate: symbol table is fixed.
- Online change: PLC operator (development tool) edits POU code and pushes change without restart. Symbol table CAN change (new variables added, existing renamed/removed). Handles for unchanged symbols stay valid; handles for removed symbols return `0x745 ReturnCodeDeviceNotifyHandleInvalid`.
- Full project download: like initial deploy. Runtime restarts. TCP connection drops. Library reconnects → reloads.

**Production frequency of online change**: rare. Production-deployed systems are typically fully recompiled and the runtime restarted. Online change is mostly a development/test action. **INVESTIGATION-NEEDED**: is online change EVER a routine production operation in our user base? Survey users before assuming "rare".

**Symbol Version (Group 0xF008)**:

- Increments when PLC symbol table changes (online change OR full deploy).
- Library's `GetSymbolVersion` reads it; `CheckSymbolVersion` compares against cached version; `RefreshSymbols` invalidates handles + reloads if changed.
- These are USER-CALLED methods. The library does NOT poll automatically.

### TwinCAT 2 runtime

- Single-task, simpler runtime.
- No online change (or limited).
- TCP connection closes on runtime restart, same as TC3.

**INVESTIGATION-NEEDED**: TC2 specifics for online change behavior. Beckhoff docs say TC2 does not support online change in the same way; but unclear if symbol table CAN ever mutate without TCP drop.

### PLC runtime restart

Trigger: TwinCAT operator clicks "Activate Configuration" or "Restart"; or the PLC is power-cycled.

Sequence from library's perspective:

1. PLC stops accepting commands; outstanding requests time out.
2. TCP socket closes (FIN or RST).
3. Library `listen` reads EOF / ECONNRESET → triggerReconnect.
4. Reconnect retries with backoff until PLC TCP listener returns.
5. New TCP connection; route may need re-registering (probe first).
6. `reloadSymbols` if symbols were loaded pre-disconnect.
7. `resubscribeNotifications` re-subscribes everything.

**Implication**: a PLC restart ALWAYS coincides with a Reconnect cycle. Cache.symbols swap (in `reloadSymbols`) is sequential with goroutines stopped. NO concurrent dispatch during the swap.

### Symbol table mutability without TCP drop

Possible only via TwinCAT online change. Frequency: rare in production. **INVESTIGATION-NEEDED**: confirm that NO other PLC operation can mutate the symbol table without dropping TCP.

Known events that DON'T drop TCP:
- Variable value changes (notification triggers).
- New variable read on-demand.
- New symbol VERSION read.

So: if symbol table changes WITHOUT TCP drop → online change occurred → user MUST call `RefreshSymbols` to invalidate stale handles. This is a deliberate API choice; the library does not auto-detect.

---

## Concurrency model — what really happens

### Operations that hold cache.lock for mutations

- `LoadSymbols`: full swap, generation++.
- `LoadSymbolsSlow`: full swap, generation++.
- `LoadSymbolList`: full swap, generation++.
- `LoadDataTypes`: replaces datatypes only, may rebuild children, generation++.
- `RefreshSymbols`: invalidates handles + calls `loadSymbols`, generation++.
- `getSymbol` on-demand insert: cache.symbols[key] = sym (no swap, no generation bump).
- `parse` (called from `handleNotification`, `ReadFromSymbol`, `ReadMultipleSymbols`): mutates Symbol fields under lock.
- `zeroOldSymbolHandles`: pre-swap invalidate.

### Operations that hold notifs.lock for mutations

- `AddSymbolNotification(s)` commit phase.
- `DeleteDeviceNotification`.
- `SumDeleteDeviceNotification` cleanup.
- `handleNotification` lookup phase (release before parse).
- `Close` notification cleanup phase.
- `Reconnect` (specifically `resubscribeNotifications` rollback).

### Concurrency scenarios actually possible

**S1 — User AddSymbolNotification + concurrent handleNotification dispatch** (HAPPENS)
- User code: AddSymbolNotification (locks notifs, commits, releases).
- Listen + recvWorker: handleNotification (locks notifs to look up handle, releases, locks cache to parse).
- Both access notifs.lock + activeNotifications. Lock serialization sufficient. **No race possible**.

**S2 — User AddSymbolNotification + concurrent LoadSymbols** (RARE — user-initiated, not part of normal flow)
- User code A: AddSymbolNotification → getSymbol (cache.lock briefly for insert) → release → PLC roundtrip → re-acquire cache.lock for fresh fetch + generation capture → release → notifs.lock for commit.
- User code B (concurrent): LoadSymbols → cache.lock + swap + generation++ → release.
- Window: between cache.lock release in A and notifs.lock acquire in A, B can swap.
- A's stale *Symbol pointer is post-swap orphaned.
- **Defense**: R-NOT-004 generation re-check + R-NOT-005 handleNotification re-resolve.
- **Frequency**: niche. Only if user code calls LoadSymbols/RefreshSymbols mid-flight while AddSymbolNotification is in progress. In benthos-umh, never. In dev/REPL, possible.

**S3 — User RefreshSymbols + concurrent handleNotification** (RARE — niche but documented API)
- User explicitly calls RefreshSymbols after detecting symbol-version change.
- handleNotification dispatching from listen/recvWorker concurrently.
- Cache.symbols swaps; activeNotifications still points at old *Symbol.
- **Defense**: R-NOT-005 (handleNotification re-resolves via FullName via cache.lock).
- **Frequency**: only if user opts into RefreshSymbols pattern. benthos-umh doesn't.

**S4 — Reconnect + user Read/Write** (PROTECTED BY DESIGN)
- User Read/Write blocks on `disconnected.Load()` and `reconnecting` wait in sendRequest.
- Reconnect's reloadSymbols runs without concurrent dispatch.
- **Defense**: lifecycle FSM (R-RECON-002).
- **Frequency**: every reconnect. But no race because lifecycle blocks user ops.

**S5 — Reconnect + active notification goroutines** (PROTECTED BY DESIGN)
- triggerReconnect cancels lifecycle.ctx.
- listen / transmit / recvWorkers exit before reloadSymbols.
- waitGroup.Wait ensures no concurrent dispatch.
- New goroutines start AFTER reload + resubscribe.
- **Defense**: lifecycle ordering (R-RECON-008).
- **Frequency**: every reconnect.

**S6 — Multiple concurrent Read/Write/Notification ops on healthy connection** (HAPPENS)
- Multiple goroutines call sendRequest concurrently. Each gets unique InvokeID. listen demultiplexes responses.
- **Defense**: R-TX-004 (per-invoke multiplexing).
- **Frequency**: continuous in production (benthos pipeline workers).

### Concurrency scenarios that CANNOT happen

- **Concurrent listen goroutines on the same Connection**: only one listen spawned per dialAndStart. Pre-Round-4 unbounded handleReceive goroutines existed but are now bounded by recvWorkerCount.
- **Read/Write during reconnect**: blocked by sendRequest's disconnected/reconnecting check.
- **Cache swap during dispatch within reconnect**: goroutines exited before swap.

---

## Requirement scenario grounding

This table maps each requirement to the concrete user scenario it defends. Requirements not grounded in a real scenario are flagged for review.

| Requirement | Real scenario | Frequency | Defense necessary? |
|-------------|---------------|-----------|---------------------|
| R-SES-002 | Initial connect | Always | Yes |
| R-SES-003 | Plugin shutdown | Always | Yes |
| R-SES-008 | Multiple goroutines detect TCP drop simultaneously | Per-disconnect | Yes |
| R-NOT-001 | Plugin uses one channel for all notifs | Always | Yes |
| R-NOT-002 | User accidentally re-subscribes same symbol | Possible, dev errors | Yes |
| R-NOT-003 | S1 with concurrent AddSymbolNotification calls (e.g. two pipelines on same conn) | Rare | Yes |
| R-NOT-004 | S2 — concurrent LoadSymbols with active subscribe | Niche (dev/REPL only) | DEFENSE-IN-DEPTH; document scenario |
| R-NOT-005 | S3 — RefreshSymbols with active notifications | Niche (online-change opt-in) | YES if RefreshSymbols is supported |
| R-NOT-006 | Slow consumer can't be allowed to stall library | Continuous (production) | Yes |
| R-NOT-007 | First-sample race within ~100ms of subscribe | Per-subscribe | Yes (cosmetic — log noise) |
| R-NOT-008 | User unsubscribes a single symbol | Possible | Yes |
| R-NOT-009 | Caller wants partial-failure visibility on batch | Always (batch path) | Yes |
| R-NOT-010 | User reuses Connection across notification sets | Niche | Yes |
| R-NOT-011 | TC2 PLC + InContext mode | TC2 only | Yes |
| R-NOT-012 | Every notification has timestamp | Always | Yes |
| R-NOT-013 | Reconnect race during resubscribe (S2-like during reconnect window) | Niche | DEFENSE-IN-DEPTH |
| R-NOT-014 | Reconnect's resubscribe partially succeeds, then fails | Possible (dual disconnect) | Yes |
| R-NOT-015 | Cleanup paths in Close, reconnect rollback | Always | Yes |
| R-CACHE-002 | Concurrent reads + writes on cache | Continuous | Yes |
| R-CACHE-003 | Generation as TOCTOU detector for S2/S3 | Niche | Defense-in-depth |
| R-CACHE-004 | Reconnect → fresh symbols, old pointers should fail-fast | Per reconnect | Yes |
| R-CACHE-006 | TC2 returns uppercase, TC3 mixed-case | Always | Yes |
| R-CACHE-007 | On-demand resolve (benthos pattern) | Continuous (benthos uses on-demand) | Yes |
| R-CACHE-008 | Lock-ordering invariant | Always | Yes (correctness) |
| R-VIEW-001 | User obtains view, reads fields | Always (user API) | Yes |
| R-VIEW-004 | User walks struct subtree | Possible (browse mode) | Yes |
| R-TX-006 | High-rate notifications | Possible (rare; could be 1000+/sec) | DEFENSE-IN-DEPTH; scenario unverified |
| R-RECON-002 | Multiple goroutines detect drop | Per disconnect | Yes |
| R-RECON-005 | Reconnect mid-batch-read | Possible | Yes |
| R-PARSE-002 | parse propagates Changed to ancestors | Continuous | Yes |
| R-PARSE-004 | WSTRING write with surrogate-pair input near truncation boundary | Edge case | Yes |
| R-PARSE-006 | Malformed PLC response | Defensive | Yes |
| R-SYM-002 | Cyclic datatype response from malformed PLC | Defensive | DEFENSE-IN-DEPTH |
| R-SUM-003 | TC2 PLC supports one sumup opcode but not the other | Confirmed via hardware | Yes |
| R-LOCK-001 | Cross-cutting concurrency rule | Always | Yes (correctness) |

### Defenses requiring explicit user-scenario validation (priority lower if unverified)

1. **R-NOT-004 + R-NOT-005 (cache.generation + handleNotification re-resolve)**:
   - Defends S2 + S3.
   - In benthos-umh's actual usage, NEVER triggers (no LoadSymbols/RefreshSymbols mid-flight).
   - Defense ONLY matters if consumer uses RefreshSymbols pattern.
   - **Action**: keep but lower priority to MEDIUM. Document as "defends user-opt-in online-change scenarios".

2. **R-TX-006 (recvWorker bounded pool)**:
   - Defends against unbounded goroutine spawn under high-rate notifications.
   - benthos-umh in practice: 1-100 symbols, typically 1Hz cycle.
   - Adversarial PLC scenario: malicious or buggy firmware bursting notifications.
   - **Action**: keep. Defense-in-depth has small cost.

3. **R-NOT-013 (resubscribe retry max 3)**:
   - Defends against permanently-flapping symbol that can't subscribe in 3 attempts.
   - In practice: such symbols indicate user config error (typo) or PLC instability.
   - **Action**: keep. Avoids infinite churn.

### Requirements without clear scenario — INVESTIGATION-NEEDED

Walk through `01-requirements.md` post this audit and tag every requirement that doesn't have a clear scenario in the table above. Mark with `INVESTIGATION-NEEDED: <question>`. Do not ship code defending these until the question is answered.

Examples (initial audit):
- **R-SES-009 (onReconnect timing)**: when should onReconnect fire — before resubscribe or after? If user expects "after" but lib fires before, semantics drift. INVESTIGATION-NEEDED: confirm contract with consumers.
- **R-RECON-007 (strict reconnect)**: who uses this? benthos-umh doesn't. Niche or vestigial? INVESTIGATION-NEEDED.
- **R-VIEW-005 (ListSymbols)**: who calls it post-LoadSymbols? In production benthos-umh doesn't. INVESTIGATION-NEEDED.

---

## PLC operating constraints (do NOT design around)

These are real PLC limits. Library SHALL NOT attempt to compensate when compensation is impossible.

### Constraint 1: TwinCAT 2 doesn't support all sumup commands
- Source: hardware test on TC2 (192.168.3.70) — 0xF085/F086 return 0x701.
- Library response: probe-and-fallback. R-SUM-003.

### Constraint 2: Notification handles are ephemeral per Connection
- Source: PROTOCOL.md, Beckhoff InfoSys.
- A handle obtained via AddDeviceNotification is valid only for THIS TCP connection. After disconnect, handle invalid.
- Library response: re-subscribe on reconnect via `resubscribeNotifications` (R-NOT-014).

### Constraint 3: Routes can't be probed without sending an ADS command
- Source: PROTOCOL.md, code-as-spec.
- The library uses GetSymbolVersion as a probe (lightweight ADS Read).
- Library response: probe → register-on-fail (R-ROUTE-004).

### Constraint 4: Beckhoff sumup soft limit ~500 sub-commands
- Source: community (Beckhoff InfoSys).
- Library response: documented but not enforced (R-SUM-005).

### Constraint 5: Plain-text route credentials over UDP
- Source: PROTOCOL.md, Beckhoff InfoSys.
- No alternative; library must accept this.
- Library response: documented limitation (R-ROUTE-003).

### Constraint 6: Symbol-version polling is user-driven
- Source: code-as-spec.
- Library does NOT background-poll for symbol version changes.
- User must call CheckSymbolVersion / RefreshSymbols periodically to detect online-change.
- Implication: library cannot "auto-handle online change" unless we add a background goroutine. Currently we DO NOT.

**INVESTIGATION-NEEDED**: should the library add background symbol-version polling (opt-in)? If yes, scope its impact on cache.lock. If no, document that online-change detection is the user's responsibility.

### Constraint 7: PLC clock vs system clock
- DeviceNotification timestamp is PLC's clock (Windows-100ns since 1601-01-01).
- May drift from system clock.
- Library response: convert to time.Time UTC; document drift possibility in godoc.

### Constraint 8: AMS Router buffer size (default 2048 KB)
- Beckhoff InfoSys.
- Sum command response must fit within this buffer.
- Library response: rely on PROTOCOL.md sanity cap (4MB) for outer packet; document the smaller per-batch limit (R-SUM-005).

---

## Investigation log

When marking `INVESTIGATION-NEEDED`, record here. Do not ship without resolution.

| Date | Question | Status | Outcome |
|------|----------|--------|---------|
| 2026-05-08 | Are there OTHER production consumers besides benthos-umh? | OPEN | Need user confirmation. |
| 2026-05-08 | Is online change EVER a routine production operation? | OPEN | Need user / community survey. |
| 2026-05-08 | TC2 specifics: can symbol table mutate without TCP drop? | RESOLVED 2026-05-08 | YES. Per Beckhoff InfoSys (tcplccontrol/925291275.html, tc3_plc_intro/2528041355.html), TC2 supports online change. Symbol table CAN mutate without TCP drop. Both TC2 and TC3 return "Notification handle is invalid" / "Symbol version is invalid" on stale handles. Library's R-NOT-005 + R-CACHE-004 defenses apply to BOTH runtimes. |
| 2026-05-08 | Are there scenarios where cache.symbols mutates without lock-ordering protection? | RESOLVED 2026-05-08 | NO. All 5 cache.symbols write sites verified under cache.lock: symbol_discovery.go:139 (LoadSymbolsSlow), :287 (getSymbol on-demand), :547 (LoadSymbolList), connection.go:689 (reloadSymbols on-demand reset), :965 (loadSymbols). |
| 2026-05-08 | onReconnect callback ordering — before or after resubscribe? | RESOLVED 2026-05-08 | AFTER. connection.go:582-583 fires `go conn.onReconnect()` after reloadSymbols + resubscribeNotifications + disconnected.Store(false) + reconnectGeneration.Add(1). R-SES-009 was correct as written. |
| 2026-05-08 | WithStrictReconnect — who actually uses this? | RESOLVED 2026-05-08 | Future users with reconnect strategies other than benthos-umh's auto-reconnect. Keep + document. |
| 2026-05-08 | Should background symbol-version polling be added? | OPEN-DESIGN | User: "very rare, but if everything runs stable we never drop connection so a potential online change would be missed, need to handle this somehow". DESIGN OPTIONS BELOW. |
| 2026-05-08 | Other production consumers besides benthos-umh? | RESOLVED 2026-05-08 | YES. Web-based ADS browser planned. Library API surface (ListSymbols, BrowseSymbols, GetSymbol on-demand) will be exercised. Keep public methods. |
| 2026-05-08 | Online change frequency in production? | RESOLVED 2026-05-08 | Very rare BUT silent failure mode unacceptable. Need detection + surfacing. |
| 2026-05-08 | Symbol.Changed + parentChanged walk: real-world purpose? | RESOLVED 2026-05-08 | DEAD CODE. Removed in commit `489834c` (DP-2). |
| 2026-05-08 | Stale handle on online change without TCP drop: detection + recovery? | DESIGNED 2026-05-08 | Three-strategy API (AutoReload default / Close / Ignore), 3-attempt cap, `WithOnSymbolVersionChanged` callback, `Update.Stale` + `Update.Reason` fields. Spec: R-CACHE-009..013, R-NOT-016..017, R-SES-011. Hardware tests pending — see DP-1. |
| 2026-05-08 | Lifecycle/cache/reconnect compliance audit | DESIGNED 2026-05-08 | TCP-drop path compliant. Online-change path covered by DP-1 (detection + strategies). Cache state machine + discovery orthogonality + RefreshSymbols + view staleness specified by DP-3 (R-CACHE-014..018). Three hardware investigations remain — see DP-3. |

## Design proposals — open for decision

### DP-1: Online-change detection — DESIGNED 2026-05-08

PLC may invalidate symbol handles without dropping TCP via online change. Library currently has no detection path; cache stays stale until user calls `RefreshSymbols` manually.

**Decision** (per user input "auto-reload but cap to avoid infinite loop / or close and let caller handle / or just surface"):

Three-strategy API. Default is auto-reload because it is the failure mode users will hit most often (rare event, expected to be transparent). Bounded reload-attempt cap defends against the user-flagged infinite-loop risk when a removed symbol cannot be re-resolved.

| Strategy | Behavior | When to choose |
|---|---|---|
| `SymbolVersionAutoReload` (default) | On 0x711/0x745/0x720: zeroOldSymbolHandles → re-discover (matching original mode) → re-resolve onDemand handles → resubscribeNotifications → onReconnect. Capped at 3 attempts in 60s sliding window; on cap exhaustion, degrade to Ignore + log WARN + invoke callback. | benthos-umh, web ADS browser, any consumer that wants transparent recovery. |
| `SymbolVersionClose` | On 0x711/0x745/0x720: mark disconnected, fire onDisconnect, stop dispatching. Caller reconstructs the connection. | Callers with their own retry/reload orchestration who want a clean lifecycle event instead of in-place reload. |
| `SymbolVersionIgnore` | Surface PLC error to caller; mark next Update for affected symbol with Stale=true + Reason. No reload, no resubscribe. | Callers that maintain their own symbol-version policy or want minimal blast radius. |

All strategies fire `WithOnSymbolVersionChanged(reason string)` callback per detection (including post-cap detections). Update struct gains `Stale bool` + `Reason string` (R-NOT-016) for consumers that want to react in the data path.

**Spec entries written**: R-CACHE-009..013, R-NOT-016..017, R-SES-011 in `01-requirements.md`.

**Default-behavior change note**: prior library surfaced 0x711 to the caller; new default auto-reloads. Users relying on the prior behavior must explicitly select `SymbolVersionIgnore`. Major-version bump justified.

**Hardware tests required before implementation lands**:

1. **TC3 — basic auto-reload**: subscribe to `MAIN.bCounter`, online-change `MAIN` to add an unrelated variable, verify next Read returns 0x711 → library auto-reloads → notification resumes within reload window. *(Validates R-CACHE-009 + R-CACHE-010)*
2. **TC3 — type change, same name**: change `MAIN.iCounter` from INT to DINT, verify PLC returns 0x711 (vs returning data interpretable as the old type). *(Validates that our 0x711 detection is sufficient and we don't need a per-symbol type-hash check)*
3. **TC3 — symbol removed**: subscribe to `MAIN.toRemove`, online-change `MAIN` removing it, verify PLC return code (expected 0x720) and that `SymbolVersionAutoReload` caps after 3 attempts and degrades to Ignore. *(Validates R-CACHE-013)*
4. **TC3 — handle reuse**: confirm whether unchanged symbols' handles stay valid post-online-change (PLC's symbol version is per-table, but observed behavior often preserves unchanged handles). Determines whether resubscribe must be all-or-nothing. *(Affects R-NOT-017 dispatch behavior)*
5. **TC2 — does TC2 emit 0x711 at all on online change?** Or only 0x745 from notifications? If TC2 only surfaces 0x745, the detection set R-CACHE-009 is correct as-written; if TC2 silently returns garbage data, we need a separate poll/version-check requirement. *(Determines whether TC2 requires the deferred Option B symbol-version polling.)*
6. **TC2 + TC3 — notification deletion event**: when PLC drops a subscribed handle (post-online-change of removed symbol), does it send a deletion notification, silently stop, or return 0x745 on next dispatch? Determines `Stale` flagging semantics in R-NOT-017. *(Open question; result feeds back into R-NOT-017 wording.)*

These tests block implementation. T-I-040..047 in `05-integration-tests.md` are the corresponding test placeholders.

### DP-2: Symbol.Changed + parentChanged removal — LANDED 2026-05-08

Option A applied. Dropped `Symbol.Changed` field, `parentChanged()` walk, `parentChangedMaxDepth`, and the `onlyChanged bool` parameter from `parseSymbol`/`GetJSON`. Build clean, race tests pass, lint clean.

Commit: `489834c` on `review/round4-fixes`. ~80 LoC removed.

### DP-3: Cache validation compliance — DESIGNED 2026-05-08

Stale-cache detection itself is covered by DP-1 (R-CACHE-009..013, R-NOT-016..017). DP-3 covers the orthogonal question: **what is the cache state machine, and where can it desync from reality?**

Decisions:

1. **Formal state machine** (R-CACHE-014). Six states — Empty, OnDemand, ListLoaded, DataTypesLoaded, ListAndDataTypesLoaded, FullyLoaded — plus Stale overlay. Transitions atomic under `cache.lock`. ListSymbols's "no full discovery" error is explicit per state.

2. **Discovery mode orthogonality** (R-CACHE-015). `LoadSymbolList` and `LoadDataTypes` independently callable, either order. `LoadSymbols`/`LoadSymbolsSlow` always end at FullyLoaded and replace the table. Calling list-loader after full-load downgrades — explicit, documented.

3. **Mixed eager + on-demand** (R-CACHE-016). Eager load after on-demand replaces the table. Prior on-demand SymbolViews remain functional via implicit re-resolve through FullName. Documented handle-leak edge case (PLC-side handles from prior on-demand resolve are not explicitly released; bounded by active count, hardware-test required to gauge severity).

4. **RefreshSymbols semantics** (R-CACHE-017). User-callable manual reload path. Eager-mode only — does NOT re-resolve `onDemandSymbols`, does NOT re-subscribe notifications. Compared to `SymbolVersionAutoReload` (DP-1) which does the full re-resubscribe pass: RefreshSymbols is the smaller hammer for callers that want manual control over the data path.

5. **SymbolView staleness via implicit re-resolve** (R-CACHE-018). Generation NOT exposed on view; foot-gun avoidance. Each access re-resolves under cache.lock — naturally observes the new pointer post-swap. R-NOT-004 stranded-symbol defense remains internal.

**Spec entries written**: R-CACHE-014..018 in `01-requirements.md`.

**Hardware investigations** (2026-05-08):

1. **Handle-leak severity** (R-CACHE-016) — RESOLVED. `TestIntegrationDP3HandleLeak` ran 20 eager-load-after-on-demand cycles on TC2 (.70) + TC3 (.224); handle values stayed identical across all cycles on both PLCs. `GetHandleByName` is idempotent per-name. No explicit release pass needed; spec invariant updated.
2. **Per-symbol vs full-cache invalidation** (cross-reference DP-1 test #4) — STILL OPEN. Requires online-change scenario. Deferred to DP-1 hardware sweep (task #31).
3. **Two-call ordering** (R-CACHE-015) — RESOLVED. `TestIntegrationDP3LoadOrderEquivalence` confirmed both orders produce identical 8-node Symbol.Children tree on TC2 + TC3.

### DP-4: WithStrictReconnect

User: "Potential future user with other reconnect strategies than benthos." Keep API as-is. Document use cases.

INVESTIGATION-NEEDED: write godoc example for the niche scenario.

Each open investigation blocks any code change that would depend on the answer.
