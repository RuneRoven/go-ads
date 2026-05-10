# go-ads Behavioral Requirements

Foundation document. Every code change, review, and test traces back to a requirement here. Sources:

- **PROTOCOL.md** — Beckhoff wire-protocol details (cited as `PROTOCOL.md§<section>`)
- **Beckhoff InfoSys** — official online documentation (cited as `InfoSys:<topic>`)
- **IEC 61131-3** — PLC language standard (symbol naming, datatypes)
- **code-as-spec** — behavior the current code embodies that has no external spec source
- **community** — observations from other ADS libraries (Beckhoff/ADS, jisotalo/ads-client, stamp/beckhoff-js)
- **incident** — bugs found in review rounds / production / hardware tests

Requirement key:

- **MUST/SHALL** — mandatory; violation is a bug.
- **SHOULD** — strongly recommended; violation needs explicit justification.
- **MAY** — optional; document the choice.

Each requirement has: priority, source, statement, invariants, verification, origin.

---

## Module SES — Session lifecycle

The Session is the user's primary managed handle. It wraps the underlying `*Client` (TCP transport + AMS routing + ADS command dispatch) and adds the symbol cache, persistent notifications, lifecycle FSM, and auto-reconnect retry loop.

### R-SES-001 — Session construction is total
- **Priority**: HIGH
- **Source**: code-as-spec
- **Statement**: `NewSession(ip, port, netid, amsPort, localNetID, localPort, requestTimeout, opts...)` (`session.go:130`) MUST NOT make network I/O. It SHALL return a `*Session` with all internal sub-types initialized (`tx`, `cache`, `notifications`, `lifecycle`, `route`). The `*Client` field is nil until `Connect()` succeeds.
- **Invariants**: `requestTimeout==0` defaults to 5s. `localPort==0` defaults to 10500. `localNetID=="auto"` or `""` leaves source.NetID as zero, to be auto-derived in Connect.
- **Verification**: T-U-001 (constructor with defaults), T-U-002 (constructor with options).
- **Origin**: Plan-C 1.5 (ctx param removed)

### R-SES-002 — Connect performs single TCP dial + optional route registration
- **Priority**: CRITICAL
- **Source**: code-as-spec, PROTOCOL.md§Frame Layout
- **Statement**: `Connect(local bool)` (`session.go:188`) SHALL dial TCP to ip:port (default 48898) within `requestTimeout`, configure aggressive keepalive (Idle=3s, Interval=2s, Count=5), optionally derive source AMS NetID from local IP, optionally register an AMS route via UDP (per R-ROUTE-004), perform local-mode handshake if `local==true`, and allocate a `*Client` whose `startWorkers()` launches exactly one `listen`, one `transmitWorker`, and `recvWorkerCount` recvWorker goroutines.
- **Invariants**: Connect failure SHALL leave the Session in a closed-but-reusable state (caller can retry Connect). On success, `IsDisconnected()==false` and the FSM is in `SessionStateConnected`.
- **Verification**: T-I-001 (TC2+TC3 happy path), T-I-002 (route already registered, skip).
- **Origin**: Plan-B Phase 1, Plan-C 1.3

### R-SES-003 — Close is idempotent and non-blocking on already-closed
- **Priority**: HIGH
- **Source**: code-as-spec
- **Statement**: `Close()` MUST be safe to call multiple times. The first call SHALL: send DeleteDeviceNotification for every active handle (best-effort), release every resolved symbol handle, cancel the lifecycle context, close the TCP socket, and wait for all goroutines (listen + transmit + recvWorkers + reconnect goroutine if running) to exit. Subsequent calls SHALL return immediately without side effects.
- **Invariants**: After Close returns, the FSM state is `SessionStateClosed` (terminal) and `sess.isClosed()==true`. No goroutine spawned by the Session or its Client is alive.
- **Verification**: T-U-010 (idempotent), T-I-003 (Close with active notifications cleans up).
- **Origin**: Plan-B Phase 1 F-01/F-02

### R-SES-004 — IsDisconnected reflects current TCP/lifecycle state
- **Priority**: MEDIUM
- **Source**: code-as-spec
- **Statement**: `IsDisconnected()` SHALL return `true` when the most recent transport operation failed AND no successful Reconnect has restored the link, OR when `Close` has been called. It SHALL return `false` when the TCP link is healthy.
- **Invariants**: The flag is observed lock-free via `atomic.Bool`; readers MAY observe a stale `false` for a brief window after a transport error before the listen goroutine sets it. Documented as "best-effort indicator, not authoritative — callers should retry on stale handles".
- **Verification**: T-I-004 (transitions on simulated TCP drop).
- **Origin**: Plan-B Phase 1 F-03

### R-SES-005 — Connect after Close requires fresh NewSession
- **Priority**: MEDIUM
- **Source**: code-as-spec
- **Statement**: A Session that has been Closed MUST NOT be Connected again. Re-use SHALL be rejected via the FSM gate `sess.isClosed()` (`session_fsm.go:191`); from the terminal `SessionStateClosed` state, no transition into `SessionStateConnecting` is allowed (`session_fsm.go:88`), so a re-Connect attempt returns an error through the existing closed-state check. Library users obtain a fresh Session via `NewSession` (`session.go:130`).
- **Verification**: T-U-011.
- **Origin**: Plan-B Phase 1

### R-SES-006 — Functional options validate at apply time
- **Priority**: MEDIUM
- **Source**: code-as-spec
- **Statement**: `WithLogger(nil)` SHALL be ignored (no-op). `WithRequestTimeout(d)` with `d<=0` SHALL be ignored. `WithRoute("")` SHALL leave route disabled. `WithBackoff(cfg)` with zero values SHALL fall back to `DefaultBackoffConfig()`.
- **Verification**: T-U-012..T-U-016.
- **Origin**: Plan-B Phase 5

### R-SES-007 — Event callbacks fire in their own goroutines
- **Priority**: MEDIUM
- **Source**: code-as-spec
- **Statement**: `WithOnDisconnect(fn)` and `WithOnReconnect(fn)` callbacks SHALL run in a goroutine separate from listen/transmit/recvWorker/reconnect (verified by T-U-017). Callbacks SHOULD NOT call Session methods — doing so risks deadlock on lifecycle locks. The library does NOT enforce the SHOULD; it is a documented godoc convention rather than a runtime contract, intentionally non-testable.
- **Verification**: T-U-017 (callback fires in separate goroutine).
- **Origin**: Plan-B Phase 1

### R-SES-008 — onDisconnect fires exactly once per disconnect event
- **Priority**: HIGH
- **Source**: incident (Plan-B F-04)
- **Statement**: When the TCP link is detected as broken, `onDisconnect` SHALL fire at most once per disconnect event, even if multiple goroutines (listen, transmit) detect the failure simultaneously. Additionally, the callback SHALL NOT fire after `Close()` has been called — lifecycle gating via `sess.isClosed()` (`session.go:540`) suppresses callback dispatch on closed sessions.
- **Invariants**: `sess.tx.disconnected.CompareAndSwap(false, true)` (`session.go:528`) is the single-detector gate; the goroutine that wins the CAS is the unique caller that fires `onDisconnect` and transitions the FSM to `SessionStateDisconnected` via `sess.transitionState(SessionStateDisconnected)` (`session.go:530`). Subsequent concurrent detectors observe `tx.disconnected==true`, skip the callback and the FSM transition. Callback dispatch is further guarded by `!sess.isClosed()` so a Close-then-drop ordering does not invoke the user callback after teardown began.
- **Verification**: T-U-018 (concurrent disconnect detectors).
- **Origin**: Plan-B Phase 1 F-04

### R-SES-009 — onReconnect fires after successful reconnect, before sendRequest unblocks
- **Priority**: MEDIUM
- **Source**: code-as-spec
- **Statement**: When Reconnect succeeds, `onReconnect` SHALL be invoked in a separate goroutine after the new TCP link + listen/transmit/recvWorkers are running, after symbols have been reloaded (if cache was loaded pre-disconnect), and after notifications have been re-subscribed.
- **Verification**: T-I-005 (TC2+TC3 simulated drop + reconnect, callback fires).
- **Origin**: Plan-B Phase 4

### R-SES-010 — Logger is required and immutable post-construction
- **Priority**: LOW
- **Source**: code-as-spec
- **Statement**: Every Session SHALL have a non-nil `*slog.Logger`. Default is `slog.Default()`. `WithLogger` overrides at construction; runtime mutation is NOT supported.
- **Verification**: T-U-019.
- **Origin**: Plan-B Phase 1

### R-SES-011 — Online-change configuration options
- **Priority**: HIGH
- **Source**: DP-1
- **Statement**: Session construction SHALL accept the following functional options governing R-CACHE-009 behavior:
  - `WithSymbolVersionStrategy(s SymbolVersionStrategy)` — selects one of `SymbolVersionAutoReload` (default), `SymbolVersionClose`, `SymbolVersionIgnore`. Values outside the enumeration SHALL be rejected at option-application time (logged + falls back to AutoReload default).
  - `WithMaxSymbolVersionReloadAttempts(n int)` — caps reload attempts for `SymbolVersionAutoReload` (default 3). `n < 1` SHALL be rejected at apply time. `n` is evaluated against a sliding window of `SymbolVersionReloadWindow`.
  - `WithSymbolVersionReloadWindow(d time.Duration)` — sliding-window length for the attempt cap (default 60s). `d <= 0` SHALL be rejected.
  - `WithOnSymbolVersionChanged(fn func(reason string))` — optional callback invoked once per detection (R-CACHE-009 codes). `reason` matches the values from R-NOT-016 (`"symbol-version-invalid"` / `"notify-handle-invalid"` / `"symbol-not-found"` / `"reload-cap-exhausted"`). Callback fires in its own goroutine (R-SES-007).
- **Invariants**: Defaults preserve current zero-config behavior for users who do not set these options, except: the auto-reload default now triggers when previously the library would have surfaced 0x711 to the caller. Users who relied on the prior surface-and-fail behavior MUST explicitly select `SymbolVersionIgnore`. This is a default-behavior change; see migration note in spec change protocol.
- **Verification**: T-U-020 (option validation), T-U-021 (callback fires on synthetic detection), T-I-047 (option round-trip on both TC2 and TC3).
- **Origin**: DP-1

---

## Module CL — Client (raw RPC layer)

The `Client` type is the Beckhoff-equivalent thin RPC layer. It owns one TCP connection, the per-invoke request multiplexing, the listen + transmit + recv worker goroutines, and the wire-format ADS commands. No symbol cache, no notification persistence, no FSM, no reconnect. Construction via `Dial`; tear-down via `Close`.

### R-CL-001 — Dial is the only constructor; Close is idempotent
- **Priority**: HIGH
- **Source**: code-as-spec, specs/09-fsm-design.md
- **Statement**: `Dial(ip, port, target, source, requestTimeout, opts...)` is the sole way to construct a usable `*Client`. It performs one TCP dial, sets TCP keepalive, starts the listen + transmit + recvWorkerCount worker goroutines, and returns. `Close()` is idempotent and returns nil; subsequent calls are no-ops. After Close, every public method returns `ErrTransportClosed`.
- **Verification**: T-U-CL-001 (Dial+Close roundtrip + double-close).
- **Origin**: Phase 5.a

### R-CL-002 — Concurrent use is safe via per-invoke multiplexing
- **Priority**: CRITICAL
- **Source**: PROTOCOL.md, code-as-spec
- **Statement**: All `Client` public methods are safe under concurrent caller invocation; the transport multiplexes via per-port queue + InvokeID. Same scheme as the legacy pre-Phase-5 Connection type.
- **Verification**: T-U-CL-002 (race detector + concurrent calls).
- **Origin**: Phase 5.b.0

### R-CL-003 — Closed Client returns ErrTransportClosed from every method
- **Priority**: HIGH
- **Source**: specs/09-fsm-design.md
- **Statement**: After `Close` (or on detection of a transport-level error such as listen-EOF or transmit-write-failure), every subsequent public method on this Client returns `ErrTransportClosed` without contacting the network. Detection points: `Client.sendRequest` early-returns when `c.tx.disconnected.Load()` is true.
- **Verification**: T-U-CL-003 (post-Close call returns ErrTransportClosed).
- **Origin**: Phase 5.c

### R-CL-004 — Goroutines are bounded
- **Priority**: HIGH
- **Source**: specs/09-fsm-design.md, R-TX-006
- **Statement**: Client SHALL NOT spawn goroutines beyond exactly one listen + one transmitWorker + recvWorkerCount recvWorkers (currently 16). No per-notification or per-call goroutines.
- **Verification**: T-U-CL-004 (goroutine count).
- **Origin**: Phase 5.a-dial

### R-CL-005 — Client carries no state that survives Close
- **Priority**: HIGH
- **Source**: specs/09-fsm-design.md
- **Statement**: After Close completes, the Client struct holds no live resources (sockets closed, goroutines exited). A re-use attempt SHALL return ErrTransportClosed; consumers obtain a new Client via Dial.
- **Verification**: T-U-CL-005 (reuse after Close).
- **Origin**: Phase 5.a

### R-CL-006 — ReleaseHandle is the symmetric counterpart to GetHandleByName
- **Priority**: MEDIUM
- **Source**: code-as-spec, specs/09-fsm-design.md
- **Statement**: `Client.ReleaseHandle(handle uint32) error` wraps `Write(GroupSymbolReleaseHandle, 0, handle-bytes)`. Provides Beckhoff-symmetric pairing with `Client.GetHandleByName`. Raw consumers manage handle lifetimes explicitly via this pair; Session-managed consumers reach the same effect via cache invalidation in `RefreshSymbols` / `loadSymbols`.
- **Verification**: T-I-CL-006 (acquire + release roundtrip).
- **Origin**: Phase 5.b.1

### R-CL-007 — Notification dispatch via callback
- **Priority**: HIGH
- **Source**: specs/09-fsm-design.md, code-as-spec
- **Statement**: Inbound DeviceNotification packets (ADS cmd 0x08) are decoded by `Client.deviceNotification` and dispatched per-sample via a callback installed via `WithNotificationHandler` (Dial-time) or `Client.SetNotificationHandler` (post-Dial). Nil handler drops packets after a Debug-level log entry. Session installs `Session.handleNotification` so cache-aware processing fires for managed consumers.
- **Invariants**: Callback runs on the recvWorker goroutine; the handler MUST NOT block indefinitely. The handler reads the Client's `notify` field under RLock; replacement via SetNotificationHandler is concurrent-safe.
- **Verification**: T-U-CL-007 (handler invoked once per sample).
- **Origin**: Phase 5.a-dial

### R-CL-008 — Drop detection callback (on-drop)
- **Priority**: MEDIUM
- **Source**: specs/09-fsm-design.md, code-as-spec
- **Statement**: When `Client.listen` or `Client.transmitWorker` observes a transport-level failure (ReadFull error, write error, oversized framing), the Client invokes a callback installed via `Client.SetOnDrop` exactly once before exiting. Raw consumers leave nil (drop is observable via `ErrTransportClosed` from subsequent RPCs); Session installs `Session.triggerReconnect`.
- **Invariants**: Callback runs on the goroutine that detected the drop; the handler MUST NOT block on Client locks (would deadlock).
- **Verification**: T-U-CL-008 (synthetic drop fires callback once).
- **Origin**: Phase 5.a-dial

---

## Module NOT — Notifications

Notifications are PLC-pushed value updates. The library subscribes via AddDeviceNotification / SumupAddDeviceNotification (0xF085), receives DeviceNotification packets (Cmd 0x0008), and dispatches to a user-provided channel.

### R-NOT-001 — All notifications on a Session share one channel
- **Priority**: HIGH
- **Source**: code-as-spec, IMPLEMENTATION.md§Concurrency
- **Statement**: All `AddSymbolNotification` calls on a single Session MUST use the same `chan *Update` receiver. Calling with a different channel while one is already set SHALL return an error.
- **Invariants**: `notifications.notificationChannel` is set on first successful subscribe; subsequent subscribes verify equality. Cleared when last notification is deleted.
- **Verification**: T-U-100 (mismatched channel rejected).
- **Origin**: Plan-B Phase 4

### R-NOT-002 — Duplicate-symbol subscribe is rejected
- **Priority**: HIGH
- **Source**: incident (R3 Errors+Concurrency F3)
- **Statement**: Calling `AddSymbolNotification` for a symbol that already has an active notification on this Session SHALL be rejected with an error. Calling `AddSymbolNotifications` (batch) MUST mark the duplicate entry as `Skipped` with `ReturnCodeDeviceNotifyHandleInvalid` and continue with non-duplicate entries.
- **Invariants**: Dedupe is case-insensitive (`strings.ToLower` / `symbolKey`). Cross-batch dedupe within the same call uses a `batchSeen` map.
- **Verification**: T-U-101 (single duplicate), T-U-102 (in-batch duplicate), T-U-103 (cross-batch duplicate).
- **Origin**: Round 3

### R-NOT-003 — TOCTOU re-check on subscribe
- **Priority**: HIGH
- **Source**: incident (R3 Architecture)
- **Statement**: `AddSymbolNotification(s)` MUST re-check the channel-mismatch and duplicate-symbol invariants after the PLC roundtrip, under `notifs.lock`, AND release any just-acquired PLC handle if the re-check fails.
- **Invariants**: The pre-check + roundtrip + post-check pattern. On post-check failure: `DeleteDeviceNotification(handle)` is called best-effort (errors logged at Warn).
- **Verification**: T-U-104 (concurrent subscribe wins post-check, loser releases handle).
- **Origin**: Round 3 fix

### R-NOT-004 — Stranded-Symbol race defense via session epoch
- **Priority**: HIGH
- **Source**: incident (R3 Errors+Concurrency, R4 Architecture)
- **Statement**: `AddSymbolNotification(s)` MUST capture `sess.epoch()` (R-RECON-005) under `cache.lock` alongside the freshly-resolved `*Symbol` pointer, then release `cache.lock`, then take `notifications.lock` and re-check `sess.epoch()` before commit (`notification_api.go:120-145, 298-318`). If the epoch advanced during the gap (a cache swap or reconnect transition fired between cache.lock release and notifications.lock acquire), the originally-resolved `*Symbol` pointer is stranded: the operation MUST mark the entry as `Skipped` with reason "cache reload during subscribe stranded symbol", surface the PLC handle so the caller (or `bestEffortDeleteNotifications`) can release it, and NOT commit to `notifications.activeNotifications`.
- **Invariants**: Capture under `cache.lock`; re-check via lock-free `epoch.Load()` while holding `notifications.lock`. The two-lock protocol respects R-CACHE-008 (locks NEVER held simultaneously). The epoch counter is shared with R-RECON-005 — false-positive Skipped due to an unrelated transition into Connected during the roundtrip is harmless (caller retries).
- **Verification**: T-U-105 (synthetic epoch bump between resolve and commit).
- **Origin**: Round 4 fix

### R-NOT-005 — handleNotification re-resolves symbol via FullName
- **Priority**: HIGH
- **Source**: incident (R3 Errors+Concurrency)
- **Statement**: When a DeviceNotification packet arrives for handle H, the dispatch code SHALL re-resolve the live `*Symbol` via `cache.symbols[symbolKey(fullName)]` under cache.lock before calling `parse`. If the symbol is no longer in the live cache, the packet SHALL be logged at Warn ("notification target symbol no longer in cache; skipping parse") and dropped.
- **Invariants**: parse runs under cache.lock against the LIVE *Symbol + LIVE datatypes, not the stranded pointer from `notifications.activeNotifications`. The activeNotifications map provides the FullName + Notification channel; cache.symbols provides the parse target. See Glossary: Stranded-Symbol.
- **Verification**: T-U-106 (synthetic stranded pointer scenario), T-I-006 (TC3 reload mid-notification).
- **Origin**: Round 4 fix

### R-NOT-006 — DeviceNotification dispatch is non-blocking on user channel
- **Priority**: HIGH
- **Source**: code-as-spec, IMPLEMENTATION.md§Concurrency
- **Statement**: `deliverNotification` SHALL use a non-blocking channel send (`select { case ch <- update: default: log warn }`). A slow or stuck consumer MUST NOT block the listen goroutine or the recvWorker pool. Drops SHALL emit a Warn log with handle + symbol identity.
- **Invariants**: `recover` covers the case of a closed channel (logs Error). Channel send happens outside any cache.lock or notifs.lock.
- **Verification**: T-U-107 (slow consumer doesn't block dispatch).
- **Origin**: Plan-B Phase 4

### R-NOT-007 — First-sample race window suppression
- **Priority**: MEDIUM
- **Source**: incident (Plan-B F-22)
- **Statement**: A DeviceNotification packet arriving for a handle that the library has just acquired but not yet committed to `notifs.activeNotifications` (race window between PLC fire and library map insert) MUST NOT generate a Warn-level "unknown handle" log. The library SHALL track `notifications.lastSubscribeNs` (atomic int64) and downgrade the log to Debug if the elapsed time since the most recent successful subscribe is less than the named constant `subscribeRaceWindowNs` (`cmd_notification.go:190`, declared as `const subscribeRaceWindowNs = int64(100 * time.Millisecond)`).
- **Invariants**: `subscribeRaceWindowNs` is the single source of truth for the suppression window — no numeric literal of "100ms" appears elsewhere in the dispatch path. Logs during shutdown/reconnect are ALSO downgraded ("expected during close/reconnect"), gated by `sess.isClosed() || sess.isReconnecting()` (`cmd_notification.go:192`).
- **Verification**: T-U-108.
- **Origin**: Plan-B Phase 4 F-22

### R-NOT-008 — DeleteDeviceNotification reverses subscribe
- **Priority**: HIGH
- **Source**: PROTOCOL.md§Cmd 0x0007
- **Statement**: `DeleteDeviceNotification(handle)` SHALL: send the PLC delete command, on success remove the handle from `notifs.activeNotifications`, remove the corresponding `NotificationConfig` (case-insensitive name match), and clear `notifs.notificationChannel` if the map becomes empty.
- **Invariants**: PLC error code 0x714 (`ReturnCodeDeviceNotifyHandleInvalid`) is treated as success-equivalent (handle already gone PLC-side).
- **Verification**: T-U-109, T-I-007.
- **Origin**: Plan-B Phase 4

### R-NOT-009 — Sum batch returns per-config result
- **Priority**: HIGH
- **Source**: incident (R2 CodeRabbit)
- **Statement**: `AddSymbolNotifications(configs, ch)` SHALL return `([]SumNotificationResult, error)` parallel to `configs`. Each `SumNotificationResult` has `Skipped error`, `Error ReturnCode`, `Handle uint32`. Caller distinguishes states:
  - `Skipped != nil` — library refused (duplicate, resolution failure, transport-aborted batch, cache stranded). Handle MAY be non-zero (PLC-accepted but library-discarded), in which case caller is responsible for `DeleteDeviceNotification(Handle)` cleanup.
  - `Skipped == nil && Error != ReturnCodeNoErrors` — PLC rejected this entry.
  - `Skipped == nil && Error == ReturnCodeNoErrors` — success; Handle is valid.
- **Verification**: T-U-110 (tri-state table), T-I-008 (batch with mixed valid/invalid symbols).
- **Origin**: Round 3

### R-NOT-010 — Notification channel set on first success
- **Priority**: HIGH
- **Source**: incident (R3 Devil's Advocate)
- **Statement**: `AddSymbolNotifications` SHALL set `notifs.notificationChannel = ch` and stamp `lastSubscribeNs` only when at least one entry succeeded. If all entries are Skipped or PLC-rejected, the channel state is unchanged.
- **Invariants**: Prevents stale channel commitment that would block future calls with a different channel.
- **Verification**: T-U-111.
- **Origin**: Round 3 fix

### R-NOT-011 — In-context transmission mode auto-fallback
- **Priority**: MEDIUM
- **Source**: PROTOCOL.md§Transmission Modes
- **Statement**: For symbols with `ContextMask==0` (single-task PLC, TC2, or no task binding), `TransModeServerCycle2` (5) and `TransModeServerOnChange2` (6) SHALL be auto-downgraded to `TransModeServerCycle` (3) / `TransModeServerOnChange` (4) before the PLC request, with a Warn log naming the original and downgraded modes.
- **Verification**: T-U-112.
- **Origin**: Plan-B Phase 4

### R-NOT-012 — Notification timestamp converts Windows-100ns to time.Time
- **Priority**: HIGH
- **Source**: PROTOCOL.md§DeviceNotification
- **Statement**: PLC `Timestamp` field (uint64, 100ns since 1601-01-01 UTC) MUST be converted to `time.Time` UTC. The library divides by `windowsTick = 10000000` and subtracts `secToUnixEpoch = 11644473600`. Timestamp == 0 SHALL be replaced with `time.Now()`.
- **Verification**: T-U-113 (unix epoch input), T-U-114 (zero timestamp).
- **Origin**: Plan-B Phase 4

### R-NOT-013 — Resubscribe re-append on reconnect Skipped
- **Priority**: HIGH
- **Source**: incident (R4 CodeRabbit #3)
- **Statement**: `resubscribeNotifications` (called during Reconnect) MUST re-append `NotificationConfig` entries returned as `Skipped` to `notifs.notificationConfigs` so the next reconnect cycle retries them. The config's unexported `resubscribeAttempts` counter SHALL increment on each Skipped event; after `resubscribeMaxAttempts == 3` retries the config SHALL be dropped with a Warn log.
- **Invariants**: Counter resets to 0 on successful subscribe (in `AddSymbolNotifications` commit path).
- **Verification**: T-U-115 (3 retries then drop), T-U-116 (success resets counter).
- **Origin**: Round 4 fix

### R-NOT-014 — Resubscribe rolls back on transport error
- **Priority**: HIGH
- **Source**: incident (Plan-B F-12)
- **Statement**: If `AddSymbolNotifications` returns a non-nil error during resubscribe (transport failure), partially-succeeded handles MUST be best-effort deleted from the PLC, client-side bookkeeping for those handles SHALL be removed, and the original `notificationConfigs` SHALL be restored so the next reconnect attempt can retry.
- **Invariants**: Snapshot `preHandles` before; diff against `activeNotifications` after to identify newly-created handles.
- **Verification**: T-I-009 (TC3 reconnect interrupted by second drop).
- **Origin**: Plan-B Phase 4 F-12

### R-NOT-015 — bestEffortDeleteNotifications for cleanup paths
- **Priority**: MEDIUM
- **Source**: code-as-spec
- **Statement**: Cleanup paths (Close, Reconnect rollback, resubscribe orphan release) SHALL use `bestEffortDeleteNotifications(handles []uint32)`. Return value is the count of successfully deleted handles. Errors are logged at Warn but never returned.
- **Verification**: T-U-117 (empty slice no-op), T-U-118 (mixed success/failure logging).
- **Origin**: Plan-B Phase 4

### R-NOT-016 — Update.Stale and Update.Reason fields
- **Priority**: HIGH
- **Source**: DP-1 (user input: "throw error in the message output or add meta")
- **Statement**: The exported `Update` struct SHALL carry two additional fields:
  - `Stale bool` — true when the dispatching library knows the value MAY be from a pre-online-change cache state (set when an `Update` is queued for delivery between detection of any R-CACHE-009 code (0x711 / 0x710 / 0x703 / 0x705 / 0x722 / 0x714) and completion of the reload, OR when strategy is `SymbolVersionIgnore` and a stale signal has been observed for this symbol).
  - `Reason string` — non-empty when `Stale=true`. One of: `"symbol-version-invalid"` (0x711), `"symbol-not-found"` (0x710), `"invalid-offset"` (0x703), `"symbol-not-active"` (0x722), `"notify-handle-invalid"` (0x714), `"invalid-size"` (0x705), `"reload-cap-exhausted"` (R-CACHE-013), `"reload-in-progress"` (queued mid-reload).
  Consumers (e.g. benthos-umh) MAY filter / log / branch on these fields. Default zero values (`false`, `""`) preserve backwards compatibility with consumers that ignore them.
- **Invariants**: Stale never spontaneously clears: once set on an Update, the Update is consumed by the user as-is. A subsequent fresh Update on the same handle (post-reload + re-resubscribe) carries Stale=false.
- **Verification**: T-U-119 (Stale set on synthetic post-detection dispatch), T-I-045 (online-change end-to-end shows Stale=true on at-most-one Update per affected symbol).
- **Origin**: DP-1

### R-NOT-017 — Notification dispatch under stale-cache strategies
- **Priority**: HIGH
- **Source**: DP-1
- **Statement**: When R-CACHE-009 detection fires:
  - Strategy `SymbolVersionAutoReload`: notification dispatch SHALL pause for affected handles during reload+resubscribe; queued samples for affected handles MAY be dropped (logged at INFO). Post-resubscribe, fresh PLC samples flow with `Stale=false`.
  - Strategy `SymbolVersionClose`: notification dispatch stops permanently; user channel closes naturally on Session.Close.
  - Strategy `SymbolVersionIgnore`: notification dispatch continues for handles the PLC still honors; the first sample observed after detection on each affected handle SHALL set `Stale=true, Reason="symbol-version-invalid"`. Subsequent samples on the same handle (PLC may still honor the OLD handle for unchanged symbols) inherit Stale=false.
- **Invariants**: A user that does not opt in to `Update.Stale` field handling is not blocked or destabilized — the field default is false.
- **Asymmetry under SymbolVersionIgnore + symbol-removed:** the dead handle's user channel receives NO further Update (terminal 0-byte sample intercepted by listener at R-CACHE-009 supplementary detection point). Surviving sibling handles on the same Session receive next-sample Stale=true Reason=detected, one-shot. Consumers waiting on a Stale terminal Update on the deleted symbol's channel SHALL instead rely on `WithOnSymbolVersionChanged` callback for that signal.
- **Verification**: T-I-046 (dispatch correctness across all three strategies).
- **Origin**: DP-1

---

## Module CACHE — Symbol cache

The symbol cache is the in-memory mirror of the PLC's symbol table.

### R-CACHE-001 — Cache is opt-in via discovery
- **Priority**: HIGH
- **Source**: code-as-spec
- **Statement**: An empty cache after Connect is valid. Symbols become known via `LoadSymbols` (full discovery, single round-trip), `LoadSymbolsSlow` (chunked), `LoadSymbolList` (browse-only), `LoadDataTypes` (datatypes only), or `GetSymbol` (on-demand single-symbol resolve).
- **Verification**: T-I-010 (LoadSymbols), T-I-011 (LoadSymbolsSlow with custom chunk size), T-I-012 (browse mode without datatypes).
- **Origin**: Plan-B + Plan-C

### R-CACHE-002 — cache.lock guards mutations
- **Priority**: CRITICAL
- **Source**: incident (Plan-C 1.4)
- **Statement**: All mutations to `cache.symbols`, `cache.datatypes`, `cache.symbolVersion`, `cache.onDemandSymbols`, `cache.symbolListLoaded`, `cache.symbolsFullyLoaded`, `cache.datatypesLoaded` MUST hold `cache.lock` (sync.Mutex). Reads MAY be lock-free where the value is independent (atomic generation counter) but in general SHALL hold the lock.
- **Invariants**: Symbol-pointer mutations to fields under cache.lock are also covered (Value, Valid, ValueParsed, LastUpdateTime).
- **Verification**: T-U-200 (race detector clean on parallel cache mutation).
- **Origin**: Plan-C 1.4

### R-CACHE-003 — epoch bumps on cache.symbols swap
- **Priority**: HIGH
- **Source**: incident (R3 Architecture, R4 final)
- **Statement**: The unified counter is `sessionFSM.epoch` (atomic.Uint64; R-RECON-005). Every operation that REPLACES `cache.symbols` SHALL call `sess.bumpEpoch()` under `cache.lock`, exactly once per swap. Swap sites:
  - `session.go:1116` — `loadSymbols` (full discovery via LoadSymbols / LoadSymbolsSlow path).
  - `session.go:789` — on-demand reset branch in `reloadSymbols` during Reconnect.
  - `tearDownAndReset` (session.go) — implicit via the subsequent dialAndStart's Connected transition (which bumps via `transitionTo`).
  - `symbol_discovery.go` — `LoadSymbolsSlow`, `LoadSymbolList`, `LoadDataTypes`, `RefreshSymbols` swap sites.
  Pure inserts (e.g. `getSymbol` creating a new entry without replacing the map) MUST NOT bump epoch; existing pointers stay valid across an insert.
- **Invariants**: Epoch captured under `cache.lock` paired with a `*Symbol` snapshot represents a consistent (epoch, ptr) tuple at capture time. Transitions into `SessionStateConnected` also bump epoch (R-RECON-005); R-CACHE-003 covers only the cache-swap bump sites.
- **Verification**: T-U-201 (LoadSymbols bumps), T-U-202 (on-demand insert does not bump).
- **Origin**: Round 4; Phase 4 unification (2026-05-08).

### R-CACHE-004 — zeroOldSymbolHandles invalidates pre-swap data
- **Priority**: HIGH
- **Source**: incident (Plan-B F-20, R3 Errors+Concurrency)
- **Statement**: Before `cache.symbols` is replaced (in loadSymbols), `zeroOldSymbolHandles(oldMap)` SHALL clear `Handle = 0`, `Value = ""`, `Valid = false`, `ValueParsed = false`, `LastUpdateTime = time.Time{}` on every old `*Symbol`. External callers holding stale pointers fail-fast on next use; cached reads within MinUpdateInterval of reconnect don't return stale pre-disconnect data.
- **Invariants**: `Notification` field on old symbols is intentionally NOT cleared (would conflict with notifs.lock ordering); old symbols are unreachable from cache.symbols anyway.
- **Verification**: T-U-203 (all fields cleared), T-I-013 (post-reconnect stale-data check).
- **Origin**: Plan-B Phase 3 F-20, Round 3 expansion

### R-CACHE-005 — onDemandSymbols tracks user-requested symbols
- **Priority**: MEDIUM
- **Source**: code-as-spec
- **Statement**: `cache.onDemandSymbols` (map[string]bool) records every symbol resolved via on-demand `GetSymbol` lookup (not via full discovery). On reconnect, `reloadSymbols` re-resolves these so user-held SymbolViews remain valid post-reconnect.
- **Invariants**: Cleared by full LoadSymbols and LoadSymbolList (which replace the symbol table entirely).
- **Verification**: T-U-204, T-I-014.
- **Origin**: Plan-B Phase 3

### R-CACHE-006 — symbolKey normalizes case
- **Priority**: HIGH
- **Source**: incident (Plan-B F-CASE), IEC 61131-3
- **Statement**: All cache map keys are derived via `symbolKey(name string) string = strings.ToLower(name)`. PLC names are case-insensitive per IEC 61131-3. TC2 returns names uppercased; TC3 preserves original casing. Lowercase keying ensures consistent lookups regardless of caller or PLC casing.
- **Invariants**: Symbol.FullName retains original casing for user-facing display. Internal map keys are always lowercase.
- **Verification**: T-U-205 (mixed-case lookup).
- **Origin**: Plan-B Phase 4

### R-CACHE-007 — getSymbol on-demand resolution
- **Priority**: HIGH
- **Source**: code-as-spec
- **Statement**: `getSymbol(name)` (internal, returns `*Symbol`) SHALL: check cache.symbols, on miss fetch via `getSymbolInfoByName` (PLC roundtrip 0xF009), acquire a Handle via `GetHandleByName`, insert into cache.symbols under cache.lock, mark in onDemandSymbols, return the pointer. If a concurrent goroutine resolved the same symbol mid-roundtrip, the duplicate handle SHALL be released to the PLC and the existing pointer returned.
- **Invariants**: Single-flight by symbol-name not enforced (each goroutine attempts independently); the duplicate-handle-release pattern reconciles after.
- **Verification**: T-I-015 (concurrent on-demand resolve releases duplicate handle).
- **Origin**: Plan-B Phase 4 F-19

### R-CACHE-008 — Lock ordering: cache.lock and notifications.lock NEVER held simultaneously
- **Priority**: CRITICAL
- **Source**: IMPLEMENTATION.md§Concurrency, incident (Plan-C 1.4)
- **Statement**: NO code path SHALL hold both `cache.lock` and `notifications.lock` at the same time. Paths that need both MUST acquire-release one, then acquire the other. The order is conventionally cache → notifications (snapshot fresh `*Symbol` pointer + `epoch` snapshot, release, take notifications to commit).
- **Verification**: T-U-206 (static analysis claim verified by code review checklist), T-U-207 (race detector run with synthetic concurrent-acquirers).
- **Origin**: Plan-C 1.4

### R-CACHE-009 — Stale cache detection on PLC online change
- **Priority**: HIGH
- **Source**: incident (DP-1), Beckhoff InfoSys (online change)
- **State machine**: detection drives the `Stale` substate of the FSM defined in R-CACHE-014; per-strategy transitions out of `Stale` are specified in R-CACHE-010..013.
- **Statement**: When the PLC executes an online change without dropping TCP, the cached `Symbol.Handle` and `cache.symbolVersion` become stale. The library SHALL treat the following PLC return codes as authoritative stale-cache signals on any read/write/notification operation. Codes verified against Beckhoff InfoSys (TC2 Utilities — ADS Return Codes table; TC2 and TC3 share the same ADS error code space):
  - `0x711 ReturnCodeDeviceSymbolVersionInvalid` (Beckhoff: `ADSERR_DEVICE_SYMBOLVERSIONINVALID`) — canonical: *"Invalid symbol version. This can occur due to an online change. Create a new handle."*
  - `0x710 ReturnCodeDeviceSymbolNoFound` (Beckhoff: `ADSERR_DEVICE_SYMBOLNOTFOUND`) — symbol lookup by name post-delete (note: library constant is misspelled "NoFound"; canonical Beckhoff name is "NotFound")
  - `0x703 ReturnCodeDeviceInvalidOffset` (Beckhoff: `ADSERR_DEVICE_INVALIDOFFSET`) — cached handle now resolves to orphaned/junk offset (TC3 v3.1.4024 returns this on read of cached handle after symbol delete + activate; see hardware finding 2026-05-10)
  - `0x722 ReturnCodeDeviceSymbolNotActive` (Beckhoff: `ADSERR_DEVICE_SYMBOLNOTACTIVE`) — Beckhoff recommendation: *"Release the handle and try again."*
  - `0x714 ReturnCodeDeviceNotifyHandleInvalid` (Beckhoff: `ADSERR_DEVICE_NOTIFYHNDINVALID`) — surfaces on AddDeviceNotification when prior handle stale post-online-change
  Detection of any of these codes SHALL trigger the strategy selected by `WithSymbolVersionStrategy` (see R-SES-011).
- **Invariants**: Detection point is the response decoder for each affected command (Read/Write/AddDeviceNotification/SumRead/SumWrite/SumAddNotification). The detection MUST NOT depend on a transport-level event because no TCP drop occurs. **Hardware finding (2026-05-10, TC3 v3.1.4024):** Notification streams (TransModeServerCycle) survive online change silently — old handle continues firing samples at new size/offset. Detection cannot rely on the notification listener for type-change or struct-offset-shift cases; it must fire from explicit ops. Symbol *removal* DOES surface via the notification listener as a 0-byte terminal sample (parser logs `parse <type>: symbol.Length N exceeds data buffer size 0`), which the detector MAY use as a supplementary signal. **Active detection** via periodic `GetSymbolVersion` poll (read of index group 0xF008) is the most reliable trigger — recommended cadence: piggyback on existing reconnect-probe path.
- **Verification**: T-I-040 (TC3 online change of in-use symbol returns 0x711, 0x703, or 0x710 to subsequent read; library reacts per strategy), T-U-220 (synthetic 0x711/0x703/0x710/0x722 in decode path triggers strategy callback).
- **Origin**: DP-1
- **SPEC FIX (2026-05-10):** Earlier draft listed `0x720` as "ReturnCodeDeviceSymbolNotFound" and `0x745` as "ReturnCodeDeviceNotifyHandleInvalid" — both hex typos. Per Beckhoff InfoSys (TC2 Utilities): `0x720 = ADSERR_DEVICE_WARNING` (signal warning, unrelated to online change), `0x745 = ADSERR_CLIENT_SYNCTIMEOUT` (client sync timeout, unrelated). Correct values: `SymbolNotFound = 0x710`, `NotifyHandleInvalid = 0x714`. Library `defs.go` constants are CORRECT (lines 302/315/316/319/333) — only this spec entry was wrong. R-NOT-016 + R-SES-011 hex references in this doc were similarly miswritten and are corrected in the same pass.
- **INVESTIGATION-NEEDED**: TC2 surface code parity — TC2 may surface 0x711 where TC3 surfaces 0x703 due to handle-table implementation differences. Detection set covers both; concrete TC2 hardware sweep still recommended before T-I-040 finalizes but not required to land #33.

### R-CACHE-010 — Strategy: SymbolVersionAutoReload (default)
- **Priority**: HIGH
- **Source**: DP-1
- **State machine**: substate of R-CACHE-014. Transition `Stale → Reloading → (back to prior loaded state)` on success, or `Stale → Reloading → Stale` (degraded to Ignore) on cap exhaustion (R-CACHE-013).
- **Statement**: Under strategy `SymbolVersionAutoReload`, on detection per R-CACHE-009 the library SHALL:
  1. Call `sess.bumpEpoch()` under `cache.lock` (R-CACHE-003) and run `zeroOldSymbolHandles` on the existing map (R-CACHE-004).
  2. Re-run the discovery mode that originally populated the cache (`LoadSymbols`, `LoadSymbolsSlow`, `LoadSymbolList`, `LoadDataTypes`, or on-demand reset for callers that never called any). For mixed populations (some symbols discovered, some on-demand), full re-discovery is run.
  3. Re-resolve handles for symbols listed in `cache.onDemandSymbols`.
  4. Re-subscribe active notifications via `resubscribeNotifications` (R-NOT-013).
  5. Fire `onReconnect` callback if registered (re-using the same callback as TCP-drop reconnect — same observable user effect).
- **Invariants**: Auto-reload runs at most `MaxSymbolVersionReloadAttempts` (default 3) times within a `SymbolVersionReloadWindow` (default 60s). On exhaustion, library transitions to the user-selected fallback strategy (default: surface error to subsequent ops; see R-CACHE-013). This bound prevents an infinite reload loop when a missing symbol persists across reloads.
- **Verification**: T-I-041 (TC3 online change auto-reload + re-subscribe completes within 5s), T-U-221 (reload-attempt cap stops at 3), T-I-042 (online change that removes a previously-subscribed symbol does NOT cause infinite reload).
- **Origin**: DP-1

### R-CACHE-011 — Strategy: SymbolVersionClose
- **Priority**: HIGH
- **Source**: DP-1
- **State machine**: substate of R-CACHE-014. Transition `Stale → Closed` (terminal).
- **Statement**: Under strategy `SymbolVersionClose`, on detection per R-CACHE-009 the library SHALL:
  1. Mark the connection disconnected (`disconnected = true`).
  2. Fire `onDisconnect` callback once (R-SES-008).
  3. Cancel any in-flight requests and stop the listen loop.
  4. Stop dispatching notifications to user channels (Update queue closes for shutdown).
  Subsequent calls to read/write/notification APIs SHALL return `ErrDisconnected`.
  Callers (e.g. benthos-umh with retry loop) detect via `IsDisconnected()` and reconstruct a fresh `Session`.
- **Invariants**: Close path is idempotent (R-SES-003). Re-Connect on the same instance is forbidden (R-SES-005).
- **Verification**: T-I-043 (online change → IsDisconnected becomes true; user can NewSession again).
- **Origin**: DP-1

### R-CACHE-012 — Strategy: SymbolVersionIgnore
- **Priority**: MEDIUM
- **Source**: DP-1
- **State machine**: substate of R-CACHE-014. Stays in `Stale` until next user-driven `RefreshSymbols`/`LoadSymbols` call exits the substate by re-entering a loaded state.
- **Statement**: Under strategy `SymbolVersionIgnore`, on detection per R-CACHE-009 the library SHALL:
  1. Surface the PLC error (any R-CACHE-009 code: 0x711 / 0x710 / 0x703 / 0x722 / 0x714) unchanged to the calling read/write op.
  2. Mark affected `Update` events with `Update.Stale = true` and populate `Update.Reason` (see R-NOT-016).
  3. NOT auto-reload, NOT auto-resubscribe.
  Caller is responsible for handling the error (call `RefreshSymbols`, reconstruct connection, etc.).
- **Invariants**: This is the smallest-blast-radius strategy. Useful for callers that maintain their own reload policy.
- **Verification**: T-I-044 (online change surfaces 0x711 to caller; no internal recovery).
- **Origin**: DP-1

### R-CACHE-013 — Reload-attempt cap with fallback
- **Priority**: HIGH
- **Source**: DP-1 (user concern: "infinite reload loop?")
- **State machine**: substate of R-CACHE-014. Caps R-CACHE-010's `Stale → Reloading → loaded` cycle; on cap exhaustion the FSM degrades to R-CACHE-012's `Stale` substate until the sliding window expires.
- **Statement**: When `SymbolVersionAutoReload` exhausts `MaxSymbolVersionReloadAttempts` (default 3) within `SymbolVersionReloadWindow` (default 60s, sliding), the library SHALL stop further auto-reload attempts and SHALL behave as `SymbolVersionIgnore` until the window slides out. The library SHALL log at WARN level and SHALL invoke `WithOnSymbolVersionChanged` callback (R-SES-011) on every detection, including post-cap detections, so callers can observe the steady-state failure.
- **Invariants**: After a successful reload, the attempt counter resets. The cap defends against a permanent PLC misconfiguration (symbol removed, never re-added) where reload would otherwise loop forever.
- **Verification**: T-U-222 (synthetic forced 0x711 every op → 3 reload attempts then degrade to ignore + log).
- **Origin**: DP-1

### R-CACHE-014 — Cache state machine (umbrella)
- **Priority**: HIGH
- **Source**: DP-3
- **Umbrella**: this entry defines the canonical state set + transition algebra. The strategy-specific entries R-CACHE-009..013 reference this state machine for their substate transitions out of `Stale`; do NOT duplicate the state-set prose into 009..013.
- **Statement**: The symbol cache occupies exactly one of these states at any instant. Transitions are atomic under `cache.lock`:
  - **Empty**: `cache.symbols == nil || len(cache.symbols) == 0` AND no discovery mode flag set. Initial state post-Connect. Read/Write/AddSymbolNotification still works via on-demand resolve (R-CACHE-007), which triggers the Empty → OnDemand transition.
  - **OnDemand**: `cache.symbols` populated only by individual `getSymbol` calls. `symbolListLoaded == false`, `symbolsFullyLoaded == false`, `datatypesLoaded == false`. `cache.onDemandSymbols` records every entry. `ListSymbols` SHALL return an error in this state (R-VIEW-005).
  - **ListLoaded**: `symbolListLoaded == true`, `symbolsFullyLoaded == false`, `datatypesLoaded == false`. Symbol table downloaded; struct/array children not expanded yet. `ListSymbols` SHALL return scalar metadata only.
  - **DataTypesLoaded**: `datatypesLoaded == true`, `symbolListLoaded == false`. Datatype table populated; symbols not. Used by callers that pre-cache datatypes for efficiency. `ListSymbols` SHALL return an error.
  - **ListAndDataTypesLoaded**: both `symbolListLoaded` and `datatypesLoaded` true; `symbolsFullyLoaded == false`. Equivalent in observable surface to FullyLoaded but reached via two-call path. `rebuildSymbolChildrenLocked` SHALL have run on each completion to expand children.
  - **FullyLoaded**: `symbolsFullyLoaded == true`. Reached via `LoadSymbols` or `LoadSymbolsSlow`. Implies `symbolListLoaded` and `datatypesLoaded` are effectively true (full discovery covers both); the explicit flags MAY remain false because the path bypassed the staged loaders.
  - **Stale**: any of the loaded states above plus an unhandled R-CACHE-009 detection. Strategy (R-CACHE-010..012) drives transition out: AutoReload → re-enters previous state; Close → transitions to Closed (no further transitions); Ignore → stays Stale until next user-driven `RefreshSymbols`/`LoadSymbols` call.
- **Invariants**: There is no "PartiallyFullyLoaded" state — `symbolsFullyLoaded` flips atomically under `cache.lock` after the entire upload + parse completes. A caller SHALL NOT observe a state where `symbolsFullyLoaded == true` but `cache.symbols` is empty.
- **Verification**: T-U-230 (state-machine table walk: assert each transition produces the expected (flag-tuple, ListSymbols-result) pair), T-I-048 (TC3 round-trip through Empty → OnDemand → FullyLoaded).
- **Origin**: DP-3

### R-CACHE-015 — Discovery mode orthogonality
- **Priority**: MEDIUM
- **Source**: DP-3
- **Statement**: `LoadSymbolList` and `LoadDataTypes` are independently callable. Either order produces the same final cache content. After both complete, `rebuildSymbolChildrenLocked` SHALL run exactly once per completion to populate `Symbol.Children` from the datatype table. `LoadSymbols` and `LoadSymbolsSlow` SHALL both result in the FullyLoaded state and SHALL discard any prior OnDemand/ListLoaded entries (full table replace per R-CACHE-003 + R-CACHE-004).
- **Invariants**: Calling `LoadSymbolList` after `LoadSymbols` SHALL replace the table again (downgrade from FullyLoaded back to ListLoaded — `symbolsFullyLoaded` SHALL be reset to false). Calling `LoadDataTypes` alone after `LoadSymbols` is a no-op for symbol content but SHALL refresh the datatype table.
- **Verification**: T-U-231 (orthogonality matrix: 4 paths through to fully-populated state produce identical cache content), T-I-049 (TestIntegrationDP3LoadOrderEquivalence — TC2 + TC3 both produce identical 8-node Symbol.Children tree regardless of LoadSymbolList/LoadDataTypes order, 2026-05-08).
- **Origin**: DP-3

### R-CACHE-016 — Mixed eager + on-demand consistency
- **Priority**: HIGH
- **Source**: DP-3 (user input: "lifecycle, cache validation and reconnect needs to be fully compliant with real behaviour")
- **Statement**: Calling `LoadSymbols`/`LoadSymbolsSlow`/`LoadSymbolList` after on-demand `GetSymbol` calls SHALL replace the entire symbol table and SHALL clear `cache.onDemandSymbols`. SymbolViews returned from prior on-demand `GetSymbol` calls SHALL be considered logically stale (their captured `*Symbol` pointer no longer matches the current cache; equivalently, `sess.epoch()` advanced after capture per R-RECON-005). Subsequent operations through those views SHALL re-resolve via `cache.symbols[symbolKey(FullName)]` — the views remain functional because re-resolve produces the new pointer.
- **Invariants**: Handles previously acquired via on-demand resolve are NOT explicitly released by the eager-load path (the table swap triggers `zeroOldSymbolHandles` which sets Handle=0). PLC behavior verified on both TC2 and TC3 (TestIntegrationDP3HandleLeak, 2026-05-08): `GetHandleByName` is idempotent for the same symbol name — the PLC returns the SAME handle value across 20 eager-load-after-on-demand cycles. The handle table is therefore bounded by distinct symbol names ever requested, NOT by reload-cycle count. No explicit release pass required.
- **Verification**: T-U-232 (post-eager-load, on-demand SymbolView still resolves to a fresh Symbol via FullName lookup), T-I-050 (TestIntegrationDP3HandleLeak — 20 cycles, handle values constant on TC2 + TC3, no leak observed).
- **Origin**: DP-3

### R-CACHE-017 — RefreshSymbols semantics
- **Priority**: HIGH
- **Source**: DP-3, code-as-spec
- **Statement**: `RefreshSymbols()` SHALL:
  1. Call `GetSymbolVersion` and compare to `cache.symbolVersion`.
  2. If unchanged: return nil immediately. No reload, no re-resolve.
  3. If changed: collect all current handles under `cache.lock`, release them via `Write(GroupSymbolReleaseHandle)` outside the lock (handle release is a network op and would deadlock under cache.lock), call `loadSymbols()` to re-discover, then update `cache.symbolVersion`.
  `RefreshSymbols` does NOT re-resolve `onDemandSymbols` and does NOT re-subscribe notifications — it is a manual eager-mode refresh, not the full reconnect path. Callers needing the full path SHALL use `Reconnect()` or rely on R-CACHE-010 (`SymbolVersionAutoReload`).
- **Invariants**: `RefreshSymbols` SHALL be safe to call concurrently with read/write/notification ops on the same Session; in-flight operations may observe intermediate state but SHALL NOT panic. Called against a `SymbolVersionClose` Session that has fired onDisconnect, `RefreshSymbols` SHALL return `ErrDisconnected`.
- **Verification**: T-U-233 (RefreshSymbols on unchanged version is a no-op), T-I-051 (TC3 online change + RefreshSymbols re-establishes cache + onDemandSymbols entries unaffected).
- **Origin**: DP-3

### R-CACHE-018 — Generation-based view staleness
- **Priority**: MEDIUM
- **Source**: DP-3
- **Statement**: `SymbolView` does NOT carry the `sess.epoch()` value at which it was captured. Stale-view detection is implicit: every operation through a view (Children, ChildrenWalk, Read via FullName) re-resolves under `cache.lock` and naturally observes the post-swap pointer. Callers MAY explicitly check freshness via `IsValid()` (R-VIEW-002) which only tests connection liveness, NOT cache epoch.
- **Invariants**: Generation tracking is internal-only (R-NOT-004 stranded-Symbol race defense) and SHALL NOT be exposed on SymbolView. Adding a `Generation` field would create a foot-gun: callers comparing it to the live counter would race against concurrent reload. The implicit re-resolve pattern is correct because it is atomic under cache.lock at each access.
- **Verification**: T-U-234 (post-reload SymbolView read returns fresh Value, not stale snapshot value).
- **Origin**: DP-3

---

## Module SYM — Symbol type

`Symbol` is the internal cache record. External callers SHOULD use `SymbolView`.

### R-SYM-001 — Symbol field guard documentation
- **Priority**: HIGH
- **Source**: code-as-spec
- **Statement**: The `Symbol` struct godoc SHALL partition its fields into:
  - **Immutable post-construction**: FullName, Name, DataType, Comment, Group, Offset, Length, BaseType, Flags, ContextMask, MinUpdateInterval, Parent, Children.
  - **Guarded by cache.lock**: Value, Valid, ValueParsed, Changed, LastUpdateTime.
  - **Handle**: write-once during `getSymbol` resolve under cache.lock; stable until `loadSymbols` zeroes the old map (also under cache.lock). Concurrent reads after resolve are safe lock-free.
  - **Guarded by notifs.lock**: Notification.
- **Verification**: T-DOC-001 (doc-string equals this contract).
- **Origin**: Round 2

### R-SYM-002 — Children/Parent tree is acyclic
- **Priority**: HIGH
- **Source**: IEC 61131-3, code-as-spec
- **Statement**: `addOffset` SHALL build a strict tree via `Parent` pointer and `Children` map. Children are constructed top-down; cycles are forbidden by IEC 61131-3 self-referencing struct rules. Defense-in-depth: depth caps in `addOffset` and `collectSubtree` SHALL prevent stack overflow on malformed PLC datatype responses.
- **Invariants**: `addOffsetMaxDepth = collectSubtreeMaxDepth = 256` (`symbols.go:415`, `symbols.go:287`).
- **Verification**: T-U-300 (synthetic cyclic datatype hits cap, logs Warn, returns).
- **Origin**: Round 4

### R-SYM-003 — Symbol.Length is the wire-data size
- **Priority**: HIGH
- **Source**: PROTOCOL.md§Symbol Entry
- **Statement**: `Symbol.Length` SHALL equal the byte count the PLC will return for a Read of this symbol. For STRING(N) types Length includes the null terminator (N+1 bytes). For WSTRING(N) types Length includes the UTF-16 null terminator (2*(N+1) bytes).
- **Verification**: T-I-016.
- **Origin**: Plan-B Phase 3

### R-SYM-004 — BaseType is ADST_ numeric type code
- **Priority**: MEDIUM
- **Source**: PROTOCOL.md§ADST_ Data Type IDs
- **Statement**: `Symbol.BaseType` (uint32) SHALL hold the PLC-reported ADST_ numeric type code (e.g. ADSTReal32=4 for REAL). For complex types (struct, array, alias) the code may be `ADST_` mapped to the underlying base; library uses both BaseType and DataType (string) for parse decisions.
- **Verification**: T-I-017.
- **Origin**: Plan-B Phase 3

### R-SYM-005 — ContextMask isolates from Flags
- **Priority**: MEDIUM
- **Source**: PROTOCOL.md§Symbol Flags
- **Statement**: `Symbol.ContextMask` (uint8) SHALL hold bits 8-11 of `Symbol.Flags` indicating PLC task context binding. ContextMask == 0 means no task binding; in-context transmission modes auto-downgrade per R-NOT-011.
- **Verification**: T-U-301.
- **Origin**: Plan-B Phase 4

### R-SYM-006 — STRING/WSTRING type-name normalization
- **Priority**: HIGH
- **Source**: code-as-spec
- **Statement**: `normalizeStringDataType(dt)` SHALL collapse type names like `"STRING(80)"` and `"WSTRING(255)"` to bare `"STRING"` and `"WSTRING"`. Other type names returned unchanged. Applied uniformly during symbol parsing so the parse switch can match on bare name.
- **Verification**: T-U-302.
- **Origin**: Plan-C 1.2

### R-SYM-007 — FullName composition rules
- **Priority**: HIGH
- **Source**: code-as-spec, incident (Plan-B F-05/F-06)
- **Statement**: When the discovery walker (`addOffset` / `addChildren`) builds a child Symbol's `FullName`, it SHALL:
  1. Empty parent segment + non-empty child segment ⇒ `FullName = child`. (Top-level symbols whose parent is the synthetic root.)
  2. Non-empty parent + non-empty child segment with no leading `[` ⇒ `FullName = parent + "." + child` (struct-member dotted form).
  3. Non-empty parent + child segment beginning with `[` ⇒ `FullName = parent + child` (array element bracket form, no separator).
  4. Empty child segment ⇒ skip; do not produce malformed names.
- **Invariants**: PLC-side casing is preserved on `FullName`; cache lookup via `symbolKey(FullName)` lowercases for comparison.
- **Verification**: T-U-303 (TestAddOffsetEmptySegmentName, TestAddOffsetFullNameWithDot, TestAddOffsetArrayFullName).
- **Origin**: Plan-B Phase 5 F-05/F-06 historic regression fixes.

### R-SYM-008 — Enum classification rules
- **Priority**: HIGH
- **Source**: code-as-spec, incident (TC3 enum expansion bug)
- **Statement**: A datatype with one or more SubItems whose names look like ENUM constants (single token, no struct-typical accessors) SHALL NOT be expanded as struct children. The walker treats it as a scalar of the underlying integer type. Specifically:
  1. If the datatype has `ArrayDim > 0`, treat as array — never as enum (ArrayDim disambiguates).
  2. Otherwise, if every SubItem name matches the enum-constant pattern, treat as scalar enum and infer base type via `inferBaseType(Size)`.
  3. On-demand mode (no `datatypes` table) SHALL still classify enums correctly when the symbol's reported `Size` matches a known integer width; otherwise fall back to BaseType.
- **Invariants**: A misclassified enum (treated as struct) corrupts every read because the walker would expand non-existent member fields. The classification rule is therefore CRITICAL for read correctness.
- **Verification**: T-U-304 (TestParseEnumNestedInStruct, TestParseEnumWithoutDatatypes, TestArrayTypedefNotMistakenForEnum).
- **Origin**: incident (TC3 enum expansion produced phantom struct children).

### R-SYM-009 — SymbolFlag bit-test helpers
- **Priority**: MEDIUM
- **Source**: code-as-spec, PROTOCOL.md§Symbol Flags
- **Statement**: `SymbolFlag.Has(flag SymbolFlag) bool` (`defs.go:126`) SHALL return true iff every bit in `flag` is set in the receiver (`f & flag == flag`). `SymbolFlag.ContextMask() uint8` (`defs.go:121`) SHALL return bits 8-11 of the flag word, right-shifted to a 0-15 nibble, encoding the PLC task context index used by InContext transmission modes (R-NOT-011). These two helpers are the only allowed access patterns for the flag word; callers MUST NOT bit-mask `SymbolFlag` values directly.
- **Invariants**: `Has` accepts compound flags (`f.Has(SymbolFlagPersistent | SymbolFlagReadOnly)` requires BOTH bits set). `ContextMask() == 0` means no task binding (single-task PLC, GVL, or top-level program variable).
- **Verification**: T-U-305 (TestSymbolFlagHas), T-U-306 (TestSymbolFlagBitValue_Detection).
- **Origin**: code-as-spec

### R-SYM-010 — Array children construction
- **Priority**: HIGH
- **Source**: code-as-spec, IEC 61131-3
- **Statement**: `makeArrayChildren(levels []datatypeArrayInfo, dataType string, totalSize uint32) map[string]*SymbolUploadDataType` (`symbols.go:591`) SHALL build a flat map of children for the topmost array level, keyed by `[i]` where `i` ranges `LBound..LBound+Elements-1`. Multi-dimensional arrays SHALL be expressed by recursing on `levels[1:]` to populate each top-level child's nested `Children` map; the final flat-map shape uses bracket-concatenated keys (`[0][1]` etc.) at the symbol-table level. `LBound` SHALL be honored for non-zero-based arrays (IEC 61131-3 permits any int32 lower bound).
- **Invariants**: Empty `levels` slice yields an empty map (defensive). `Elements == 0` at any level yields an empty map (also defensive — TwinCAT does not declare zero-element arrays in practice). `Elements > maxArrayElementsPerLevel` (1_000_000) is rejected with an Error log (defends against malformed PLC datatype responses). `LBound + Elements > MaxUint32` is rejected (overflow guard).
- **Verification**: T-U-307 (TestMakeArrayChildren_HappyPath), T-U-308 (TestMakeArrayChildren), T-U-309 (TestMakeArrayChildrenEmpty), T-U-310 (TestMakeArrayChildrenNonZeroLBound), T-U-311 (TestMakeArrayChildren_2D), T-U-312 (TestMakeArrayChildren_3D), T-U-313 (TestMakeArrayChildren_ZeroElements).
- **Origin**: code-as-spec

### R-SYM-011 — addChildren no-overwrite invariant
- **Priority**: MEDIUM
- **Source**: code-as-spec
- **Statement**: `addChildren(symbol *Symbol, symbols map[string]*Symbol)` (`symbols.go:377`) SHALL recursively register every descendant of `symbol` into the flat `symbols` map keyed by `symbolKey(child.FullName)`. If a key already exists from a prior insertion (multi-pass discovery, structural alias), addChildren SHALL NOT overwrite the existing entry and SHALL NOT recurse into the would-be-duplicate's children either (the existing entry's children were already registered by the original insertion).
- **Invariants**: Defends against accidental shadowing during discovery sequencing — a top-level symbol that shares a name with a nested struct member must keep its top-level pointer in the map.
- **Verification**: T-U-314 (TestAddChildren), T-U-315 (TestAddChildrenNoDuplicates).
- **Origin**: code-as-spec

---

## Module VIEW — SymbolView

`SymbolView` is the public read-only handle returned by `GetSymbol` / `ListSymbols`.

### R-VIEW-001 — SymbolView is a snapshot
- **Priority**: HIGH
- **Source**: incident (R3 Architecture, R4 Devil's Advocate)
- **Statement**: A SymbolView captures metadata + Value + Valid (renamed `Parsed` if Round-4 cosmetic applied) at view-creation time, under cache.lock. All field reads are lock-free struct reads. The view is internally consistent: caller cannot observe a `Parsed=true, Value=empty` tear.
- **Invariants**: After concurrent `loadSymbols` swap, the view shows pre-swap state. For fresh data, caller MUST call `GetSymbol` again or subscribe via `AddSymbolNotification`.
- **Verification**: T-U-400 (snapshot consistency under concurrent mutation).
- **Origin**: Round 3 (post-revert from live methods)

### R-VIEW-002 — IsValid distinguishes live from zero-value
- **Priority**: MEDIUM
- **Source**: code-as-spec
- **Statement**: `SymbolView.IsValid()` SHALL return `false` for the zero-value SymbolView (no backing connection, no FullName) and `true` for views obtained from `GetSymbol`/`ListSymbols`/`Children`/`ChildrenWalk`.
- **Verification**: T-U-401.
- **Origin**: Round 3

### R-VIEW-003 — Children returns direct-children snapshot map
- **Priority**: MEDIUM
- **Source**: code-as-spec
- **Statement**: `Children()` SHALL return a freshly-allocated `map[string]SymbolView` of direct children, captured under cache.lock. Returns `nil` for scalars or for symbols whose subtree has not been populated. Each child SymbolView is itself a snapshot (recursive call to `view()`).
- **Invariants**: Caller can mutate the returned map without affecting library state. Multiple calls to Children may see different cache states if reload occurred between.
- **Verification**: T-U-402.
- **Origin**: Round 3

### R-VIEW-004 — ChildrenWalk collect-then-iterate
- **Priority**: HIGH
- **Source**: incident (R3 ChildrenWalk footgun)
- **Statement**: `ChildrenWalk(fn)` SHALL: under cache.lock, collect all descendants into a `[]SymbolView`; release cache.lock; invoke `fn(view) bool` for each entry until `fn` returns false. The user `fn` MUST be safe to call any Session method (no lock held during invocation).
- **Invariants**: `collectSubtree` recursion has 256-depth cap (`collectSubtreeMaxDepth = 256`, `symbols.go:287`) matching `addOffsetMaxDepth`.
- **Verification**: T-U-403 (fn calls back into Session without deadlock), T-U-404 (cycle hits cap).
- **Origin**: Round 4

### R-VIEW-005 — ListSymbols requires full discovery
- **Priority**: MEDIUM
- **Source**: code-as-spec
- **Statement**: `ListSymbols()` SHALL return error if `cache.symbolsFullyLoaded == false`. On success returns `map[string]SymbolView` keyed by PLC-cased FullName.
- **Verification**: T-U-405 (error before LoadSymbols), T-I-018 (after LoadSymbols).
- **Origin**: Plan-B Phase 3

### R-VIEW-006 — GetSymbol resolves on-demand
- **Priority**: HIGH
- **Source**: code-as-spec
- **Statement**: `GetSymbol(name)` SHALL return a SymbolView for `name`, resolving via on-demand PLC lookup if not in cache. Errors propagate from `getSymbol`. The returned view's Handle is non-zero on success.
- **Verification**: T-I-019.
- **Origin**: Plan-C 1.5

### R-VIEW-007 — Reading view fields after Close is allowed (snapshot semantics)
- **Priority**: LOW
- **Source**: code-as-spec
- **Statement**: A SymbolView field-read is lock-free and uses captured data. Reads after `Session.Close()` are not undefined behavior — they return the snapshot value. `Children()`/`ChildrenWalk()` after Close MAY return nil (cache cleared) or stale (cache untouched by Close); caller behavior post-Close is best-effort.
- **Verification**: T-U-406.
- **Origin**: code-as-spec

---

## Module TX — Transport

The transport layer owns the TCP socket, the request/response multiplexing, and the inbound/outbound goroutine channels.

### R-TX-001 — Single TCP connection per Session
- **Priority**: CRITICAL
- **Source**: code-as-spec
- **Statement**: A Session (and its underlying `*Client`) shares a single `*transport`; that transport holds exactly zero or one `net.Conn` at any moment, guarded by `tx.connMu` (sync.Mutex). Reconnect SHALL replace the TCP connection atomically: close-old, dial-new, swap under lock. Post-Phase-5 the `*Client` is reallocated on each successful redial (`session.go:894`); the `*transport` pointer is reused so callers retain a stable identity.
- **Verification**: T-U-500 (no leaked sockets across reconnects).
- **Origin**: Plan-B Phase 1

### R-TX-002 — bufio framing on read and write
- **Priority**: HIGH
- **Source**: code-as-spec
- **Statement**: `listen` reads via `bufio.NewReader(conn.tx.connection)` to support split AMS-TCP-headers. `transmitWorker` writes via `bufio.NewWriter` with explicit Flush per packet. Header is always 6 bytes (1 reserved + 1 system + 4 length-LE).
- **Verification**: T-U-501 (split-packet listen test).
- **Origin**: Plan-B Phase 1 F-05

### R-TX-003 — AMS-TCP packet length sanity cap
- **Priority**: HIGH
- **Source**: incident (Plan-B F-09)
- **Statement**: A declared AMS-TCP `Length` exceeding `4 * 1024 * 1024` (4 MiB) SHALL trigger `triggerReconnect` instead of allocating the buffer. Defense against attacker-controlled or wire-corrupt length fields that would otherwise allocate gigabytes.
- **Verification**: T-U-502 (oversize header → reconnect, no panic).
- **Origin**: Plan-B Phase 2 F-09

### R-TX-004 — Per-invoke request multiplexing
- **Priority**: HIGH
- **Source**: PROTOCOL.md§AMS Header
- **Statement**: Outbound requests increment `tx.currentRequest` (atomic.Uint32) for each new InvokeID. The client tracks `tx.activeRequests[invokeID] = chan []byte` under `tx.activeRequestLock`. Inbound responses match by InvokeID and forward the body via the channel; unknown InvokeIDs log Error and are dropped.
- **Verification**: T-U-503 (concurrent requests; correct correlation), T-U-504 (unknown InvokeID dropped).
- **Origin**: Plan-B Phase 1

### R-TX-005 — sendRequest retries on context cancellation only during reconnect
- **Priority**: HIGH
- **Source**: code-as-spec
- **Statement**: Post-Phase-5 `Client.sendRequest` (`client.go:517`) is itself non-retrying: it early-returns `ErrTransportClosed` when `c.tx.disconnected.Load()` is true (the transport-level "no live socket" predicate, equivalent to `Session.isTransportDown()`), otherwise issues exactly one round-trip and surfaces transport-mid-flight failures as `context.Canceled` / `context.DeadlineExceeded`. Retry-on-context-cancellation is implemented at the Session layer in helpers like `readFromSymbolRetry` (`symbol_access.go:61`) and `readMultipleSymbolsRetry` (`symbol_access.go:144`): when a request fails AND a reconnect is in flight (FSM helper `sess.isReconnecting()` returns true, i.e. `lifecycle.state == SessionStateReconnecting`) AND the Session is not closed (`!sess.isClosed()`), the helper SHALL call `sess.waitForReconnect()` (`session_fsm.go:235`, blocks on `lifecycle.reconnectDone`) and retry exactly once with the post-reconnect Client. Retry SHALL NOT fire on `context.DeadlineExceeded` from the caller's request-timeout, nor on errors observed when no reconnect is in flight.
- **Invariants**: The legacy `lifecycle.reconnecting atomic.Bool` field was removed in Phase 3.c; readiness is FSM-driven via `isReconnecting()`. Note that `sendRequest` uses `isTransportDown()` (the `tx.disconnected` flag) NOT `isDisconnected()` (the FSM-level predicate that returns true for the entire `SessionStateReconnecting` window) — between a successful redial and the symbol-reload phase the transport IS alive even though the FSM is still Reconnecting, and `dialAndStart` (`session.go:879`) clears `tx.disconnected` immediately after dial so the reload itself can use sendRequest. Mixing these two predicates would deadlock the reconnect path.
- **Verification**: T-U-505 (retry on canceled), T-U-506 (no retry on deadline).
- **Origin**: Plan-B Phase 1; Phase 5 (sendRequest moved to Client; Session retains retry semantics in symbol-access helpers).

### R-TX-006 — Bounded recvWorker pool dispatches non-system packets
- **Priority**: HIGH
- **Source**: incident (R4 Devil's Advocate LB-1)
- **Statement**: Inbound non-system AMS packets (Length>0, System==0 in TCP header) SHALL be enqueued to `tx.recvQueue` (buffered chan []byte size `recvQueueSize=256`). `recvWorkerCount=16` worker goroutines SHALL consume the queue and dispatch via `handleReceive`. On queue overflow (workers saturated), listen SHALL drop the packet with a Warn log naming queue size and worker count.
- **Invariants**: Workers tracked via `lifecycle.waitGroup`. Listen never spawns goroutines per packet (was the pre-Round-4 behavior).
- **Verification**: T-U-507 (overflow drop), T-I-020 (high-rate notification handling).
- **Origin**: Round 4

### R-TX-007 — System packets use shared systemResponse channel
- **Priority**: HIGH
- **Source**: code-as-spec
- **Statement**: AMS-TCP packets with `System > 0` (handshake responses) SHALL be forwarded to `tx.systemResponse` (buffered) for the local-mode handshake. NOT shared with normal request/response multiplexing.
- **Verification**: T-U-508.
- **Origin**: Plan-B Phase 1

### R-TX-008 — chanMu protects channel reassignment
- **Priority**: HIGH
- **Source**: incident (Plan-C 1.4)
- **Statement**: `tx.sendChannel`, `tx.systemResponse`, `tx.recvQueue` SHALL be reassigned only under `tx.chanMu` (sync.RWMutex). Readers (sendRequest, listen) take RLock; reconnect takes write Lock for swap.
- **Verification**: T-U-509 (race detector on concurrent reconnect + sendRequest).
- **Origin**: Plan-C 1.4

---

## Module RECON — Reconnect FSM

### R-RECON-001 — Auto-reconnect default behavior
- **Priority**: HIGH
- **Source**: code-as-spec
- **Statement**: `WithAutoReconnect(true)` (default) means transport-detected drops automatically launch `Reconnect()` in a separate goroutine. `WithAutoReconnect(false)` means the library marks `disconnected=true` and returns errors from `sendRequest`; user code MUST call `Reconnect()` manually.
- **Verification**: T-I-021 (auto), T-I-022 (manual).
- **Origin**: Plan-B Phase 5

### R-RECON-002 — triggerReconnect is single-flight
- **Priority**: CRITICAL
- **Source**: incident (Plan-B F-04)
- **Statement**: `triggerReconnect` SHALL CAS `tx.disconnected` from false to true (`session.go:528`); the first detector wins the CAS, transitions the FSM to Disconnected, fires onDisconnect, and (when auto-reconnect enabled) launches the reconnect goroutine. Subsequent concurrent detectors observe `tx.disconnected==true`, skip the callback and reconnect launch. `Reconnect()` itself is gated by `state.transitionToOnce(SessionStateReconnecting)` (`session.go:569`); `transitionToOnce` returns ok=false on idempotent re-entry, which is the single-flight gate that prevents concurrent retry loops.
- **Invariants**: `lifecycle.reconnectDone` channel is allocated by triggerReconnect (or Reconnect itself if entered directly) and closes via deferred close in Reconnect when the retry loop exits (success or fail). `tx.disconnected` lives on `*transport` (`transport.go:36`); the legacy `lifecycle.disconnected` and `lifecycle.reconnecting` atomic.Bool fields no longer exist post-Phase-3.c.
- **Verification**: T-U-600 (concurrent triggers spawn one goroutine).
- **Origin**: Plan-B Phase 1

### R-RECON-003 — Backoff is stepped + capped
- **Priority**: MEDIUM
- **Source**: code-as-spec
- **Statement**: `BackoffConfig` SHALL define stepped intervals: InitialInterval (1s) for InitialAttempts (3), then MidInterval (5s) for MidAttempts (3), then SlowInterval (15s) for SlowAttempts (4), then MaxInterval (30s) thereafter. Backoff SHALL reset on every successful reconnect.
- **Verification**: T-U-601.
- **Origin**: Plan-B Phase 5

### R-RECON-004 — maxReconnectAttempts limit
- **Priority**: MEDIUM
- **Source**: code-as-spec
- **Statement**: `WithMaxReconnectAttempts(n)` with n>0 SHALL cap total reconnect attempts. After n failures, Reconnect returns error and the connection stays disconnected. n==0 (default) means infinite retries.
- **Verification**: T-I-023.
- **Origin**: Plan-B Phase 5

### R-RECON-005 — Epoch counter for stranded-handle detection
- **Priority**: HIGH
- **Source**: code-as-spec
- **Statement**: A single unified counter `epoch atomic.Uint64` lives on `sessionFSM` (`session_fsm.go:97`, `session_fsm.go:102-106`). It replaces the prior `cache.generation` and `lifecycle.reconnectGeneration` fields (Phase 4 consolidation). Read via `sess.epoch()` (`session_fsm.go:213`); bump via `sess.bumpEpoch()` (`session_fsm.go:221`). Bump triggers:
  1. Every transition INTO `SessionStateConnected` — bumped inside `sessionFSM.transitionTo` / `transitionToOnce` (`session_fsm.go:139-141, 168-170`). Covers both initial Connect (`session.go:341`) and successful Reconnect (`session.go:675`).
  2. Every explicit `cache.symbols` swap performed under `cache.lock` outside a Connected-entry transition: `loadSymbols` (`session.go:1116`), the on-demand reset branch in `reloadSymbols` (`session.go:789`), and the discovery swaps in `symbol_discovery.go` (`LoadSymbolsSlow`, `LoadSymbolList`, `LoadDataTypes`, `RefreshSymbols`).
  Retry helpers (`readFromSymbolRetry`, `readMultipleSymbolsRetry`) capture epoch pre-roundtrip; if it differs post-error, they re-resolve handles via getSymbol and retry.
- **Invariants**: A single counter encodes both reconnect events and cache-swap events. False-positive retries (e.g. an unrelated swap during one reconnect) are harmless. Pure inserts via on-demand getSymbol do NOT bump (existing pointers stay valid across an insert) — only swaps and Connected-entries bump.
- **Verification**: T-I-024 (reconnect mid-read triggers retry).
- **Origin**: Plan-B Phase 4 F-21; Phase 4 unification (2026-05-08).

### R-RECON-006 — Reconnect re-establishes route + symbols + notifications
- **Priority**: HIGH
- **Source**: code-as-spec
- **Statement**: Successful Reconnect SHALL: dial new TCP, perform local-mode handshake if applicable, ensure route (probe + register-if-missing or force-register depending on options), reload symbols (in the same mode that was loaded pre-disconnect: full / list-only / datatypes-only / on-demand-only), re-subscribe notifications via `resubscribeNotifications`, fire `onReconnect` callback, and transition the FSM to `SessionStateConnected` (which bumps `sess.epoch()` per R-RECON-005).
- **Verification**: T-I-025 (TC3 simulated drop + reconnect, all state restored).
- **Origin**: Plan-B Phase 4

### R-RECON-007 — Strict reconnect mode
- **Priority**: MEDIUM
- **Source**: code-as-spec
- **Statement**: `WithStrictReconnect(maxAttempts)` SHALL fail Reconnect when any previously-resolved on-demand symbol is no longer available on the PLC (online change removed it). Default (without strict) skips missing symbols gracefully. Strict mode supports up to maxAttempts retries before connection closes; 0 means fail immediately on first missing symbol.
- **Verification**: T-I-026.
- **Origin**: Plan-B Phase 5

### R-RECON-008 — sync.WaitGroup add/wait ordering
- **Priority**: CRITICAL
- **Source**: incident (Plan-B F-02)
- **Statement**: `dialAndStart` SHALL re-check `sess.isClosed()` (`session.go:880`) before `lifecycle.waitGroup.Add(2 + recvWorkerCount)`. Close calls waitGroup.Wait BEFORE re-init of channels/ctx. The reconnectDone channel coordinates: Close waits for reconnectDone before waitGroup.Wait so the Add happens-before Wait deterministically.
- **Invariants**: The `closed bool` / `lifecycle.closed.Load()` field no longer exists post-Phase-3.b; the FSM helper `isClosed()` (`session_fsm.go:191`) is the single source of truth for terminal-Closed observation. It returns true iff `lifecycle.state` has reached `SessionStateClosed`.
- **Verification**: T-U-602 (Close during dial doesn't trigger sync.WaitGroup misuse), T-U-603 (race detector clean).
- **Origin**: Plan-B Phase 1 F-02

### R-RECON-009 — Reconnect goroutine exits via reconnectDone
- **Priority**: HIGH
- **Source**: code-as-spec
- **Statement**: Reconnect exits via `defer close(reconnectDone)` regardless of success/failure path. Pending sendRequest callers waiting on this channel are released.
- **Verification**: T-U-604.
- **Origin**: Plan-B Phase 1

### R-RECON-010 — disconnected flag flips false in dialAndStart
- **Priority**: MEDIUM
- **Source**: code-as-spec
- **Statement**: `dialAndStart` SHALL call `sess.tx.disconnected.Store(false)` after successful dial + before goroutine launch (`session.go:672, 879`). Documented as "acceptable: small window where IsDisconnected returns false but listen/transmit not yet running; callers retry on stale handles".
- **Verification**: T-U-605.
- **Origin**: Plan-B Phase 1 F-03

---

## Module CMD — ADS commands

Single-shot ADS commands per PROTOCOL.md§ADS Commands.

### R-CMD-001 — ReadDeviceInfo (Cmd 1)
- **Priority**: HIGH
- **Source**: PROTOCOL.md§Cmd 0x0001
- **Statement**: `ReadDeviceInfo()` SHALL send Cmd=1 with empty body, parse a 24-byte response into `DeviceInfo{Major, Minor, Version, DeviceName[16]}`. Used for TC2 vs TC3 detection by Major version.
- **Verification**: T-I-027.
- **Origin**: Plan-B Phase 1

### R-CMD-002 — Read (Cmd 2)
- **Priority**: HIGH
- **Source**: PROTOCOL.md§Cmd 0x0002
- **Statement**: `Read(group, offset, length)` SHALL send Cmd=2 with 12-byte body (group + offset + length), parse 4-byte error code + length + data response. Returns the data slice on success.
- **Verification**: T-I-028.
- **Origin**: Plan-B Phase 1

### R-CMD-003 — Write (Cmd 3)
- **Priority**: HIGH
- **Source**: PROTOCOL.md§Cmd 0x0003
- **Statement**: `Write(group, offset, data)` SHALL send Cmd=3 with header (group+offset+length) + data body, parse 4-byte error code response.
- **Verification**: T-I-029.
- **Origin**: Plan-B Phase 1

### R-CMD-004 — ReadState (Cmd 4)
- **Priority**: MEDIUM
- **Source**: PROTOCOL.md§Cmd 0x0004
- **Statement**: `ReadState()` SHALL send Cmd=4, parse `States{ADSState ADSState, DeviceState uint16}` from the response.
- **Verification**: T-I-030.
- **Origin**: Plan-B Phase 1

### R-CMD-005 — ReadWrite (Cmd 9)
- **Priority**: HIGH
- **Source**: PROTOCOL.md§Cmd 0x0009
- **Statement**: `WriteRead(group, offset, readLen, writeData)` SHALL send Cmd=9 with header (group+offset+readLen+writeLen) + writeData body, parse error+length+data response. Used internally by SumRead/SumWrite/SumNotif.
- **Verification**: T-I-031.
- **Origin**: Plan-B Phase 1

### R-CMD-006 — AddDeviceNotification (Cmd 6) wire format
- **Priority**: HIGH
- **Source**: PROTOCOL.md§Cmd 0x0006
- **Statement**: `AddDeviceNotification(group, offset, length, transMode, maxDelay, cycleTime)` SHALL send 40-byte body: group(4)+offset(4)+length(4)+transMode(4)+maxDelay-100ns(4)+cycleTime-100ns(4)+reserved(16). Response: 4-byte error + 4-byte handle.
- **Invariants**: maxDelay/cycleTime converted from time.Duration: `d.Nanoseconds() / 100`.
- **Verification**: T-I-032.
- **Origin**: Plan-B Phase 4

### R-CMD-007 — Body length validation
- **Priority**: HIGH
- **Source**: incident (Plan-B F-19)
- **Statement**: Single-command response handlers (Read/ReadDeviceInfo/etc.) SHALL validate that the response body length equals the declared header `Length`, OR fail-fast with error if unparseable. Defends against truncated PLC responses.
- **Verification**: T-U-700.
- **Origin**: Plan-B Phase 4 F-19

### R-CMD-008 — invokeID is per-request random for route registration
- **Priority**: HIGH
- **Source**: incident (Plan-B F-24)
- **Statement**: Route registration UDP packets (port 48899) SHALL use `crypto/rand`-derived 32-bit invokeID per call. The PLC echoes invokeID; the response parser validates equality and rejects on mismatch (defends against UDP spoofing on local network).
- **Verification**: T-U-701.
- **Origin**: Plan-B Phase 4 F-24

### R-CMD-009 — ReturnCode implements the error interface
- **Priority**: MEDIUM
- **Source**: code-as-spec
- **Statement**: `ReturnCode` (uint32, `defs.go:242`) SHALL implement the `error` interface via `func (rc ReturnCode) Error() string` (`defs.go:566`). `Error()` returns the human-readable message for the code (e.g. `0x711` → `"symbol version invalid"`). Callers MAY `return rc` directly when surfacing PLC error codes, and `errors.Is(err, ReturnCodeDeviceNotifyHandleInvalid)` works as expected.
- **Invariants**: `ReturnCodeNoErrors` (0x00) is still a valid `error` value; callers SHOULD compare to `ReturnCodeNoErrors` before propagating, since "no error" should not be wrapped as `error != nil`.
- **Verification**: T-U-702 (TestReturnCodeError — verifies error interface satisfied + message text per code).
- **Origin**: code-as-spec

---

## Module SUM — Sum/batch commands

PROTOCOL.md§Sum Commands documents the wire protocol for batch operations.

### R-SUM-001 — SumRead three-tier fallback
- **Priority**: HIGH
- **Source**: IMPLEMENTATION.md§Sum Command Fallback Strategy
- **Statement**: `SumRead(requests)` SHALL try opcodes in order: `SumReadEx2 (0xF084)` → `SumReadEx (0xF083)` → individual reads. Cached state in `capabilities.sumReadCmd` (atomic.Uint32) tracks which opcode is supported. Resets on Reconnect.
- **Verification**: T-I-033 (TC3 uses Ex2), T-I-034 (TC2 uses Ex), T-I-035 (forced individual fallback).
- **Origin**: Plan-B Phase 2

### R-SUM-002 — SumWrite single fallback
- **Priority**: HIGH
- **Source**: IMPLEMENTATION.md§Sum Command Fallback Strategy
- **Statement**: `SumWrite(requests)` SHALL try `0xF081` once; on `ReturnCodeServiceNotSupported (0x701)` falls back to individual writes. State in `capabilities.sumWriteState`.
- **Verification**: T-I-036.
- **Origin**: Plan-B Phase 2

### R-SUM-003 — SumAddDeviceNotification per-opcode state
- **Priority**: HIGH
- **Source**: incident (R2 critical: shared-state poisoning)
- **Statement**: `SumAddDeviceNotification (0xF085)` and `SumDeleteDeviceNotification (0xF086)` SHALL track separate `capabilities.sumAddNotifState` and `sumDeleteNotifState` atomics. A PLC may support one but not the other; sharing state would poison the working opcode after a transient failure on the other.
- **Verification**: T-U-800 (independent CAS), T-I-037 (TC2 fallback for both, TC3 native for both).
- **Origin**: Round 1 critical fix

### R-SUM-004 — SumNotificationResult tri-state
- See R-NOT-009.

### R-SUM-005 — Sum command 500-item soft limit
- **Priority**: MEDIUM
- **Source**: Beckhoff InfoSys
- **Statement**: Beckhoff recommends ≤ 500 sub-commands per sum request. The library SHALL NOT enforce this hard cap (silent splitting could change observable behavior); instead document in godoc and let callers enforce. Larger batches MAY succeed but risk PLC-side jitter and AMS router buffer overflow (default 2048KB).
- **Verification**: T-DOC-002 (godoc mentions 500), T-I-038 (N=500 succeeds on TC3).
- **Origin**: community (Beckhoff InfoSys)

### R-SUM-006 — parseSumReadResponse data-section integrity
- **Priority**: HIGH
- **Source**: incident (Round 4 verification)
- **Statement**: `parseSumReadResponse` SHALL: read N×8 metadata header (error+length per item), then read variable-length data section. For each item:
  - If `Error == NoErrors`: validate `Length == requests[i].Length` (oversize and undersize are both protocol drift; cascade-mark remaining items invalid and break).
  - If `Error != NoErrors`: still advance `dataOffset += Length` (PLC may emit data even for failed items; misalignment otherwise), guarded by overflow check.
  - Bounds checks use uint32 arithmetic to avoid 32-bit int wrap.
- **Verification**: T-U-801 (oversize/undersize/error+data).
- **Origin**: Round 3 + 4 fixes

### R-SUM-007 — executeSumCommand orchestrates fixed-size sums
- **Priority**: LOW
- **Source**: code-as-spec
- **Statement**: Generic `executeSumCommand[Req,Res](spec)` orchestrates state-check → encode → WriteRead → decode → fallback for fixed-size sum operations (Add/Delete notification). SumRead/SumWrite intentionally NOT migrated due to two-cmd-ID fallback (Read) and variable data section (Write).
- **Verification**: covered by T-I-033..T-I-037.
- **Origin**: Plan-C 1.2

### R-SUM-008 — symbolSumAddress prefers handle-based addressing
- **Priority**: HIGH
- **Source**: code-as-spec, incident (TC2/TC3 sum-read bug)
- **Statement**: When building a sum-command address tuple for a `*Symbol`, `symbolSumAddress(sym)` SHALL:
  1. If `sym.Handle != 0`, return `(GroupSymbolValueByHandle, sym.Handle)` (handle-based addressing — works on both TC2 and TC3 inside sum reads).
  2. Otherwise, if `sym.Group != 0`, return the direct group + accumulated absolute offset (walks the Parent chain, summing each ancestor's `Offset` into the child's `Offset`).
  3. Otherwise (Handle == 0 AND Group == 0): return handle-based addressing with handle 0; the PLC will return an error for handle 0, surfacing the misuse.
- **Invariants**: Handle preference matters because direct group+offset addressing with process-image groups (e.g. 0x4040) FAILS inside sum reads on some TwinCAT versions, even when the absolute offset is mathematically correct. Handle-based addressing is universally accepted.
- **Verification**: T-U-805 (TestSymbolSumAddress_PrefersHandleOverDirect, _HandleOnlyNoGroup, _DirectFallbackNoHandle, _DirectFallbackChildAccumulatesOffset, _DirectFallbackNestedChild).
- **Origin**: incident (TC2 sum-read failed on direct addressing of struct members).

---

## Module ROUTE — UDP route registration

### R-ROUTE-001 — UDP port 48899 for route registration
- **Priority**: HIGH
- **Source**: PROTOCOL.md§Route Registration
- **Statement**: Route registration uses UDP packets to PLC port 48899 (independent of TCP port 48898). Packet format: cookie(4) + invokeID(4) + serviceID(4) + AMSAddr(8) + tagCount(4) + tags...
- **Verification**: T-U-900 (packet build), T-I-039 (TC3 register).
- **Origin**: Plan-B Phase 1

### R-ROUTE-002 — invokeID echo validation
- See R-CMD-008.

### R-ROUTE-003 — Route credentials transmitted in plaintext
- **Priority**: LOW (documented limitation)
- **Source**: PROTOCOL.md§Route Registration, code-as-spec
- **Statement**: Beckhoff's protocol transmits route registration credentials in plaintext over UDP. There is no encrypted alternative. The library SHALL document this limitation in `WithRoute` godoc; user code is responsible for ensuring a trusted network.
- **Verification**: T-DOC-003.
- **Origin**: Plan-B Phase 4

### R-ROUTE-004 — Connect probes route before registering
- **Priority**: MEDIUM
- **Source**: code-as-spec
- **Statement**: `Connect` (and Reconnect) SHALL probe the PLC via `GetSymbolVersion` first; if the probe succeeds, route is assumed registered and registration is skipped. On probe failure, the library registers the route with credentials. After `routeProbeFailures >= 3`, force-register on every (re)connect (PLC may have rebooted or route table changed).
- **Verification**: T-I-040.
- **Origin**: Plan-B Phase 5

### R-ROUTE-005 — WithForceRouteRegistration option
- **Priority**: LOW
- **Source**: code-as-spec
- **Statement**: `WithForceRouteRegistration()` SHALL skip the probe step and always register the route (with credentials) on every Connect/Reconnect. For environments where routes are non-persistent.
- **Verification**: T-I-041.
- **Origin**: Plan-B Phase 5

---

## Module PARSE — Wire parsing

### R-PARSE-001 — parse() walks Symbol tree
- **Priority**: HIGH
- **Source**: code-as-spec
- **Statement**: `(symbol *Symbol).parse(data, offset, datatypes)` SHALL traverse `symbol.Children` recursively for struct/array types, decode primitive types per `parseableTypes`, and call `updateValue` to commit. Mutates Symbol fields; MUST run under cache.lock.
- **Verification**: T-U-1000 (primitive types), T-U-1001 (nested struct).
- **Origin**: Plan-B + Plan-C

### R-PARSE-003 — STRING write null-termination
- **Priority**: HIGH
- **Source**: PROTOCOL.md§STRING
- **Statement**: STRING write SHALL truncate input to `symbol.Length - 1` bytes (reserving 1 byte for null terminator), zero-fill the buffer, copy truncated bytes. The buffer is `symbol.Length` bytes; last byte is always 0.
- **Verification**: T-U-1004 (exact length, over length, under length).
- **Origin**: Plan-B Phase 3

### R-PARSE-004 — WSTRING surrogate-pair-aware truncation
- **Priority**: HIGH
- **Source**: incident (Round 3 fix)
- **Statement**: WSTRING write encodes via `utf16.Encode([]rune(value))`. Truncation to `maxChars = (symbol.Length - 2) / 2` MUST drop a trailing high surrogate (0xD800-0xDBFF) if truncation lands on it; otherwise the PLC receives an unpaired surrogate.
- **Verification**: T-U-1005 (surrogate-pair input + tight Length).
- **Origin**: Round 3

### R-PARSE-005 — STRING parse stops at first NUL
- **Priority**: HIGH
- **Source**: code-as-spec
- **Statement**: STRING parse SHALL find the first NUL byte (`bytes.IndexByte`) within `[start:stop]` and treat that as the string terminator. Bytes after NUL are ignored. If no NUL found, the entire range is the string.
- **Verification**: T-U-1006.
- **Origin**: Plan-B Phase 3

### R-PARSE-006 — symbol.Length oversize protection in parse
- **Priority**: HIGH
- **Source**: incident (Plan-B F-16)
- **Statement**: parse SHALL reject `symbol.Length > len(data)` in uint64 space (avoiding 32-bit int wrap for Length=0xFFFFFFFF). Returns error before allocation/slice.
- **Verification**: T-U-1007.
- **Origin**: Plan-B Phase 3 F-16

### R-PARSE-007 — Type-specific parse for parseable primitives
- **Priority**: HIGH
- **Source**: code-as-spec
- **Statement**: parse SHALL handle each entry in `parseableTypes`: BOOL, BYTE, SINT, USINT, INT, UINT, INT16, UINT16, DINT, UDINT, INT32, UINT32, REAL, LREAL, INT64, UINT64, ULINT, LINT, TIME, DATE, DT, DATE_AND_TIME, TIME_OF_DAY, TOD, STRING, WSTRING. Returns formatted string per type's natural representation (true/false, decimal, ISO timestamps).
- **Verification**: T-U-1008..T-U-1027 (one per type).
- **Origin**: Plan-B Phase 3

### R-PARSE-008 — NetID string parsing
- **Priority**: HIGH
- **Source**: code-as-spec, PROTOCOL.md§AMS Header
- **Statement**: `stringToNetID(s string) ([6]byte, error)` (`ams.go:30`) SHALL parse a literal NetID string of the form `"a.b.c.d.e.f"` into the 6-byte AMS NetID array. Each segment MUST be a decimal integer in the range 0..255. The helper SHALL return a wrapped error on malformed input: wrong segment count, non-numeric segment, or out-of-range value. Callers (`NewSession`, `WithRoute`) handle the empty / `"auto"` cases before invoking this helper; this function assumes a literal, well-formed-shape NetID.
- **Verification**: T-U-704 (TestStringToNetID — happy path), T-U-705 (TestStringToNetIDErrors — segment count, non-numeric, out-of-range).
- **Origin**: Plan-B Phase 1

### R-PARSE-009 — Bit-addressed read/write helpers
- **Priority**: HIGH
- **Source**: code-as-spec, PROTOCOL.md§Process-Image Access
- **Statement**: `ReadBit(data []byte, bitIndex int) bool` (`symbol_codec.go:538`) SHALL return the value of the bit at `bitIndex`, where bit 0 is the least-significant bit of the first byte. `WriteBit(data []byte, bitIndex int, value bool)` (`symbol_codec.go:550`) SHALL set or clear that bit IN PLACE, MUST NOT clobber adjacent bits in the same byte, and MUST NOT modify any other byte. Both helpers are bounds-checked: `bitIndex < 0` or `bitIndex/8 >= len(data)` is silently ignored (return false / no-op). Used by BOOL-in-byte process-image access (groups 0x4020 / 0x4040).
- **Verification**: T-U-706 (TestReadBit_Extract), T-U-707 (TestReadBit_AllPositions — every bit in a byte), T-U-708 (TestWriteBit_Set), T-U-709 (TestWriteBit_Clear), T-U-710 (TestWriteBit_PreservesOthers — write must not clobber adjacent bits).
- **Origin**: code-as-spec

### R-PARSE-010 — writeToNode / parse type-inference fallback
- **Priority**: HIGH
- **Source**: code-as-spec, incident (struct-write round-trip on opaque types)
- **Statement**: When `Symbol.DataType` is not in `parseableTypes` and is not resolvable via the `datatypes` table (R-PARSE-011), `parse` (`symbol_codec.go:202-214`) and `writeToNode` (`symbol_codec.go:328-341`) SHALL invoke `inferBaseType(symbol.Length)` (`symbol_codec.go:263`) to map the byte size to a primitive integer type: `1 → SINT`, `2 → INT`, `4 → DINT`, `8 → LINT`. If `Length` does not match a known integer width, parse/writeToNode SHALL return an error of the form `"data type %q not parseable and size %d not inferable (must be 1/2/4/8 bytes)"`.
- **Invariants**: Inference always returns a SIGNED integer type; unsigned enums and REAL/LREAL targets WILL be misinterpreted (the read path emits a Warn log; same on write). Parse and writeToNode SHALL log identical Warn messages so the failure mode is symmetric.
- **Verification**: T-U-711 (TestWriteToNode_FallsBackToInferredType), T-U-712 (TestWriteToNode_RejectsUnknownTypeWithUninferableSize), T-U-713 (TestSymbolParseUnknownType), T-U-714 (TestWriteToNodeUnknownType).
- **Origin**: code-as-spec

### R-PARSE-011 — Type alias resolution via datatypes map
- **Priority**: HIGH
- **Source**: code-as-spec, incident (TC3 enum / type alias parse)
- **Statement**: When `Symbol.DataType` is not in `parseableTypes` and the cache's `datatypes` table has an entry whose underlying `DataType` IS in `parseableTypes` (e.g. `MyEnum32` → `DINT`), `parse` (`symbol_codec.go:172-185`) and `writeToNode` (`symbol_codec.go:321-327`) SHALL resolve the alias by recursing with the underlying scalar type. If the `datatypes` table is nil (on-demand mode) or the alias points at a non-parseable type, the call SHALL fall through to `inferBaseType` (R-PARSE-010).
- **Invariants**: Alias resolution is a one-shot table lookup, not a recursive chain — TwinCAT's datatype table flattens transitive aliases at upload time, so a single lookup suffices. Aliases that ALSO have struct children fall under R-PARSE-001 (struct walk takes precedence).
- **Verification**: T-U-715 (TestWriteToNodeAliasResolution), T-U-716 (TestWriteToNodeAliasWithoutDatatypes — nil-table fallback), T-U-717 (TestSymbolParseAliasResolution).
- **Origin**: code-as-spec

---

## Module LOCK — Cross-cutting concurrency

### R-LOCK-001 — NEVER hold cache.lock and notifications.lock simultaneously
- See R-CACHE-008. Reiterated for cross-cutting visibility.

### R-LOCK-002 — Race detector clean on -race build
- **Priority**: CRITICAL
- **Source**: code-as-spec
- **Statement**: All shipped code SHALL pass `go test -race ./...` clean. CI MUST run race-detector tests.
- **Verification**: CI pipeline.
- **Origin**: ongoing

### R-LOCK-003 — atomic types for lock-free coordination
- **Priority**: HIGH
- **Source**: code-as-spec
- **Statement**: Where mutex synchronization would be too coarse, the library uses:
  - `atomic.Bool` — `tx.disconnected` (`transport.go:36`; legacy "no live socket" flag, R-TX-005);
  - `atomic.Int64` — `notifications.lastSubscribeNs` (R-NOT-007);
  - `atomic.Uint32` — `tx.currentRequest` (R-TX-004), `capabilities.sumReadCmd` / `sumWriteState` / `sumAddNotifState` / `sumDeleteNotifState` (R-SUM-001..R-SUM-003);
  - `atomic.Uint64` — `sessionFSM.epoch` (`session_fsm.go:105`; the unified counter that replaced the prior `cache.generation` and `lifecycle.reconnectGeneration` fields, see R-RECON-005);
  - `atomic.Uint32` — `sessionFSM.state` (the FSM state word, accessed via `transitionTo` / `transitionToOnce` / helpers like `isClosed()` / `isReconnecting()`);
  - `atomic.Int32` — `route.routeProbeFailures`.
  Reads/writes of these MUST go through atomic methods or the dedicated FSM helpers (`sess.epoch()`, `sess.bumpEpoch()`, `sess.isClosed()`, `sess.isDisconnected()`, `sess.isReconnecting()`, `sess.isTransportDown()`).
- **Invariants**: The legacy `lifecycle.closed atomic.Bool` and `lifecycle.reconnecting atomic.Bool` fields no longer exist post-Phase-3.b/3.c — terminal-Closed and Reconnecting observations both go through the FSM. Bumping `epoch` either happens implicitly on every transition INTO `SessionStateConnected` (see R-RECON-005) or explicitly via `sess.bumpEpoch()` at cache-swap sites (see R-CACHE-003).
- **Verification**: T-U-1100 (mutex-vs-atomic boundary verified), race detector.
- **Origin**: Plan-B + Plan-C; Phase 4 unification (epoch); Phase 5 (Client extraction).

### R-LOCK-004 — secret type defends credential leak via fmt/slog
- **Priority**: MEDIUM
- **Source**: incident (Plan-B F-25)
- **Statement**: Internal `secret string` type implements `String() string` returning `"[REDACTED]"` and `slog.LogValuer.LogValue()` returning the same. Any accidental `fmt.Sprintf("%+v", conn)` or `slog.Any("conn", conn)` SHALL NOT leak the route password.
- **Verification**: T-U-1101.
- **Origin**: Plan-B Phase 4 F-25

### R-LOCK-005 — Lock granularity per sub-type
- **Priority**: HIGH
- **Source**: IMPLEMENTATION.md§Architecture
- **Statement**: After Plan-C 1.4, locks are per-aggregate:
  - `cache.lock` (Mutex) — symbolCache state
  - `notifs.lock` (Mutex) — notificationManager state
  - `tx.connMu` (Mutex) — transport.connection
  - `tx.chanMu` (RWMutex) — transport channels
  - `tx.activeRequestLock` (Mutex) — request map
  - `lifecycle.ctxMu` (RWMutex) — ctx + shutdown
  - `lifecycle.reconnectMu` (Mutex) — reconnectDone channel
- **Verification**: T-DOC-004 (each lock has documented field-set guard).
- **Origin**: Plan-C 1.4

---

## Cross-cutting non-functional requirements

The R-NFR-001..004 entries (release / CI / hardware-test policy) live in `02-quality-constitution.md` because they govern release process, not library runtime behaviour. See that document's "Release & CI policy (R-NFR-NNN)" section.

---

## Glossary

Tight definitions for jargon used in this document. Linked from first-use sites.

- **Stranded-Symbol** — A `*Symbol` pointer captured before a cache swap (e.g. via `GetSymbol` or notification subscribe), then dereferenced after the swap committed a new map. The old pointer still points at valid memory but has been detached from the live cache. Defended via the `(epoch, *Symbol)` capture pattern: capture `sess.epoch()` under `cache.lock` alongside the pointer, recheck `epoch.Load()` before commit; mismatch ⇒ skip + release handle. See R-NOT-004, R-RECON-005, R-CACHE-016.

- **First-sample race window** — Interval between AddDeviceNotification ACK (PLC accepted the subscription) and arrival of the first DeviceNotification packet. During this window the dispatcher may receive a packet for a handle that has not yet been recorded in `notifications.activeNotifications` (the ACK return path lost the race with the first sample). Logged at Debug instead of Warn for `subscribeRaceWindowNs` (=100ms) after `lastSubscribeNs`. See R-NOT-007.

- **In-context transmission** — `Client.sendRequest` path: the synchronous wire-op hot path used by Read/Write/AddDeviceNotification etc. Lives on `*Client`, wrapped by `*Session` for retry semantics. Uses `isTransportDown()` (TCP-level liveness) rather than `isDisconnected()` (FSM-level), because during the Reconnecting state the transport may be alive and re-loading symbols while the FSM has not yet transitioned back to Connected. See R-TX-005.

- **(gen, ptr) tuple** — Atomic pairing of an `epoch` snapshot (`gen`) and a `*Symbol` pointer (`ptr`), captured under `cache.lock`. The tuple is the unit of stranded-Symbol defense: at use site, recheck `sess.epoch() == gen`; mismatch ⇒ ptr is stranded, abort. The `gen` half is shared with reconnect-generation logic (R-RECON-005) — false-positives from unrelated FSM transitions are harmless (caller retries). See R-NOT-004, R-CACHE-016.
