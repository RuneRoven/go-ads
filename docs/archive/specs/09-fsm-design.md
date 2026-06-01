# Layered architecture: Client + Session, with FSM in Session

> Status: IMPLEMENTED — Phases 1–5 landed 2026-05-08 / 2026-05-09. The
> doc remains as the canonical design reference. Phase 6 (DP-1
> strategies wired as FSM transitions) is pending hardware tests
> (task #31).

## Why this exists

Two failures in the current `Connection` design motivate this rework:

1. **Mixed concerns.** The current type owns transport + addressing + symbol
   cache + notification persistence + capability detection + route registration
   + reconnect FSM + lifecycle callbacks + logger. Beckhoff's reference lib
   keeps these strictly separated (`AdsDevice` → `AmsConnection` → `TcpSocket`)
   and is far simpler as a result. Our value-add is the session/recovery
   layer; that's worth keeping but should not pollute the raw RPC layer.

2. **Implicit FSM.** Lifecycle was encoded in five concurrency primitives
   plus two channels plus two atomic counters, scattered across
   `reconnector.go`, `connection.go`, and `comm.go`. Cross-cutting bug
   fixes had repeatedly added ordering rules between these primitives
   (R-SES-008, R-SES-009, R-RECON-005..010, R-NOT-013, R-NOT-014). The
   flag soup was an FSM — but an implicit one. Reviewers had to
   reconstruct the state graph from scattered CAS calls. Phases 1–3
   replaced this with the explicit `SessionState` enum and the
   `sessionFSM` helper in `session_fsm.go`; the legacy boolean flags
   are gone.

This document specifies a two-layer architecture:

- **Layer 2 — `Client`** is the Beckhoff-equivalent: thin, independent RPC
  functions over a transport. No cache. No reconnect. No subscription
  persistence. No FSM. Each method either succeeds or returns a wire error;
  drops fail subsequent calls. Suitable for one-shot consumers (CLI tools,
  the planned web-based ADS browser doing a quick probe) and as the building
  block for Layer 3.

- **Layer 3 — `Session`** wraps a `Client` and layers on the value-add:
  symbol cache, notification persistence, auto-reconnect, online-change
  strategies, lifecycle callbacks, and the explicit FSM described later in
  this doc. Suitable for long-running consumers (benthos-umh).

The FSM lives *only* in Session. Client is stateless w.r.t. lifecycle — its
"state" is the transport's state, full stop.

## Layer 2: Client (raw RPC)

`Client` is the Beckhoff-style thin wrapper. Public surface mirrors
`AdsDevice` plus the explicit RPCs we need that Beckhoff's lib does not
expose directly.

### Construction and lifetime

```
client, err := ads.Dial(ip, target, source, opts...)
defer client.Close()
```

`Dial` opens one TCP connection, runs the local AMS handshake, and starts the
listen + transmit goroutines. `Close` shuts down the goroutines and the
socket. There is no `Reconnect`. If the socket drops, every subsequent call
returns `ErrTransportClosed` and the caller must construct a new `Client`.

Lifetime states are exactly two: **alive** (transport up) and **closed**
(transport down for any reason). No FSM enum required at this layer.

### Public methods (sketch — final API set in the implementation plan)

```
Read(group, offset uint32, length int)             ([]byte, error)
Write(group, offset uint32, data []byte)            error
ReadWrite(group, offset uint32, readLen int, write []byte) ([]byte, error)
ReadDeviceInfo()                                    (DeviceInfo, error)
ReadState()                                         (State, error)
WriteControl(adsState, deviceState uint16, data []byte) error

GetHandleByName(name string)                        (uint32, error)
ReleaseHandle(handle uint32)                        error

AddDeviceNotification(group, offset uint32, attrib NotificationAttrib, ch chan<- *Update) (uint32, error)
DeleteDeviceNotification(handle uint32)             error

GetSymbolVersion()                                  (uint8, error)
GetSymbolUploadInfo()                               (SymbolUploadInfo, error)
DownloadSymbolList()                                ([]byte, error)
DownloadDataTypes()                                 ([]byte, error)

SumRead(reqs []SumReadReq)                          ([]SumReadRes, error)
SumWrite(reqs []SumWriteReq)                        ([]SumWriteRes, error)
SumAddDeviceNotification(reqs []SumNotifReq)        ([]SumNotifRes, error)
SumDeleteDeviceNotification(handles []uint32)       ([]uint32, error)
```

### What Client deliberately does NOT do

- No symbol cache. `GetHandleByName` is a fresh RPC every call. (Beckhoff:
  same behavior.)
- No name-based read/write. Caller does `GetHandleByName` then
  `Read(GroupSymbolValueByHandle, handle, length)` themselves. (Beckhoff:
  same.)
- No notification persistence. The handle returned by `AddDeviceNotification`
  is valid only until TCP drop. After drop, it's invalid and the caller
  must re-register against a new Client.
- No `Reconnect`. Drops are terminal at this layer.
- No `IsDisconnected`. Caller treats `ErrTransportClosed` from any method
  as the disconnection signal.
- No DP-1 online-change handling. Stale signals (0x711/0x745/0x720) are
  surfaced as ordinary wire errors; caller decides what to do.
- No `onReconnect` or `onDisconnect` callbacks. Lifecycle is observed via
  return values.

### Spec invariants for Client (sketch — full set added to `01-requirements.md`)

- C-CL-001: `Dial` is the only constructor. `Close` is idempotent.
- C-CL-002: every public method is safe under concurrent use; the transport
  layer multiplexes via per-port queue + invokeID identical to today's
  scheme.
- C-CL-003: a closed Client returns `ErrTransportClosed` from every public
  method.
- C-CL-004: Client SHALL NOT spawn goroutines beyond the listen + transmit
  workers.
- C-CL-005: Client carries no state that survives `Close`.

## Layer 3: Session (managed, with FSM)

`Session` wraps a `Client` and adds everything benthos-umh and other
long-running consumers need.

### Construction and lifetime

```
sess, err := ads.NewSession(ip, target, source, opts...)
err = sess.Connect()                  // dials underlying Client
defer sess.Close()
```

`NewSession` does no I/O — it just builds the struct (mirrors today's
`NewConnection`). `Connect` is what actually dials the underlying Client.
After `Close`, the Session is terminal; a new Session must be constructed
to talk to the PLC again.

The Session owns a `*Client` field protected by an interior mutex. On TCP
drop, the Session's reconnect FSM creates a fresh `*Client` and swaps it
into the field. Existing user-held references (handles, notification
channels) survive because they reference Session, not Client.

### What Session adds on top of Client

| Feature                                         | Where it lives          |
|-------------------------------------------------|-------------------------|
| Symbol cache (R-CACHE-014 state machine)        | `Session.cache`         |
| `GetSymbol` on-demand resolve with caching      | `Session.GetSymbol`     |
| `LoadSymbols`/`LoadSymbolsSlow`/`LoadSymbolList`/`LoadDataTypes` | `Session.Load*`         |
| `RefreshSymbols`                                | `Session.RefreshSymbols`|
| `AddSymbolNotification(s)` (name-based, persisted) | `Session.notifs`        |
| Notification persistence + auto-resubscribe      | `Session.notifs.notificationConfigs` |
| Auto-reconnect (`autoReconnect`, backoff, `onDisconnect`/`onReconnect`) | `Session.lifecycle` (FSM) |
| DP-1 online-change strategies (R-CACHE-009..013) | `Session.lifecycle` FSM |
| `Update.Stale` + `Update.Reason`                | `Session.notifs` dispatch |
| `IsDisconnected()`, `State()`                   | `Session.lifecycle` FSM |
| `WithStrictReconnect`, `WithSymbolVersionStrategy`, `WithOnSymbolVersionChanged` | `Session.opts` |

The full FSM specification follows below. It applies only to Session.

## Migration from current `Connection`

This is a breaking API change. We do not ship a backwards-compatibility alias.

- Current `Connection` is renamed to `Session`. The name `Connection` is
  removed.
- Every option named `WithX` that referred to connection-level behavior is
  re-evaluated for which layer it belongs to. Most stay on Session
  (auto-reconnect, callbacks, strategies). A few become Client construction
  options (transport timeouts, request timeout, logger).
- The new `Client` type is *additive* — it has no equivalent in current
  code, so there is nothing to migrate from.
- `NewConnection` → `NewSession`. `Connect` / `Close` / `Reconnect` keep
  their names on Session.
- benthos-umh and any other in-tree consumer (e.g. the planned web ADS
  browser) update imports + type names in one pass.
- DP-1 online-change handling lands inside Session. Client surfaces stale
  signals (0x711 / 0x745 / 0x720) raw.

This change ships in v2.1. The current v2 is deprecated at the same release;
the package version line in `go.mod` is bumped accordingly. Consumers
pinning v2.0.x are not auto-upgraded; they migrate explicitly.

---

# FSM design (Session layer)

The remainder of this document specifies the Session-layer FSM. It assumes
the layering above. After this lands, Session-internal lifecycle has these
properties:

- Lifecycle state lives in **one** atomic field: `state Lifecycle` (uint32 enum).
- Transitions occur via a **single** internal helper that holds the FSM mutex,
  validates the from-state, advances, and fires hook callbacks.
- The reconnect-generation and cache-generation counters merge into **one**
  `epoch atomic.Uint64`, bumped on every Connected re-entry.
- DP-1 strategies (`SymbolVersionAutoReload` / `SymbolVersionClose` /
  `SymbolVersionIgnore`) become FSM transitions, not separate flag paths.

## States

Seven named states. Exactly one is active at any instant.

| State            | Meaning                                                                                                |
|------------------|--------------------------------------------------------------------------------------------------------|
| `Constructed`    | `NewConnection` returned. No TCP. No goroutines. Connect is the only legal next call.                  |
| `Connecting`     | `Connect` running first dial + handshake + initial route registration.                                 |
| `Connected`      | Steady state. TCP up. Listen + transmit goroutines running. Read/Write/notification API serves normally.|
| `Reloading`      | Cache reload in flight (DP-1 AutoReload, RefreshSymbols, or explicit LoadSymbols-on-live-conn). TCP still up. |
| `Disconnected`   | TCP drop detected, no reconnect attempt yet. Transient — immediately advances to Reconnecting if `autoReconnect`, else terminal-style (Closed). |
| `Reconnecting`   | Dial + handshake + reload + resubscribe in progress after a drop. Possibly looping under backoff.      |
| `Closed`         | Terminal. `Close` called or unrecoverable failure. No transitions out.                                 |

State diagram:

```
                    +----------------+
                    |  Constructed   |
                    +-------+--------+
                            |
                       Connect()
                            |
                            v
                    +----------------+
                    |   Connecting   |
                    +-------+--------+
                            |
                  handshake | OK
                            v
        +--------------------------------------+
        |              Connected               |
        +--+----+--------------------------+---+
           |    |                          |
           |    | reload trigger           | TCP drop /
           |    | (DP-1 AutoReload         | listen worker
           |    |  / RefreshSymbols /      | observes EOF
           |    |  user LoadSymbols)       |
           |    v                          v
           | +-----------+        +----------------+
           | | Reloading |        |  Disconnected  |
           | +-----+-----+        +-------+--------+
           |       |                      |
           |    OK | (re-enter)           | autoReconnect=true
           |       v                      v
           |   Connected           +----------------+
           |       ^               |  Reconnecting  |
           |       |               +--+----------+--+
           |       | reconnect OK     |          |
           |       +------------------+          | exhausted /
           |                                     | Close called
           |                                     v
           |                           +----------------+
           +-------- Close() --------> |     Closed     |
                                       +----------------+
```

## Events

Events are the things that trigger transitions. They come from three sources:

| Source         | Events                                                                                              |
|----------------|-----------------------------------------------------------------------------------------------------|
| User API call  | `Connect`, `Close`, `LoadSymbols`/`LoadSymbolList`/`LoadDataTypes`/`LoadSymbolsSlow`, `RefreshSymbols`, `Reconnect` |
| TCP listener   | `dropDetected` (EOF, write failure, framing error)                                                  |
| Decoder        | `staleSignalDetected` (response carries 0x711 / 0x745 / 0x720 — DP-1)                                |
| Reconnect FSM  | `reconnectAttemptDone(success bool)`, `reconnectAttemptsExhausted`                                  |
| DP-1 cap       | `reloadAttemptsExhausted`                                                                           |

## Transition table

Format: `(from-state, event, guard) → to-state, side effects`.

| #  | From            | Event                              | Guard                                                | To              | Side effects                                                                       |
|----|-----------------|------------------------------------|------------------------------------------------------|-----------------|------------------------------------------------------------------------------------|
| 1  | Constructed     | `Connect`                          | —                                                    | Connecting      | dial transport, start listen + transmit                                            |
| 2  | Connecting      | handshake OK                       | —                                                    | Connected       | bump `epoch`, fire `onReconnect` IF this is post-drop reconnect                    |
| 3  | Connecting      | handshake fail                     | `autoReconnect == false`                             | Closed          | tear down                                                                          |
| 4  | Connecting      | handshake fail                     | `autoReconnect == true && attempts < max`            | Reconnecting    | apply backoff                                                                      |
| 5  | Connecting      | handshake fail                     | `attempts >= max`                                    | Closed          | tear down, fire `onDisconnect` (final)                                             |
| 6  | Connected       | user `Close`                       | —                                                    | Closed          | tear down                                                                          |
| 7  | Connected       | `dropDetected`                     | `autoReconnect == true`                              | Disconnected    | fire `onDisconnect` (CAS-once via state guard)                                     |
| 8  | Connected       | `dropDetected`                     | `autoReconnect == false`                             | Closed          | fire `onDisconnect`, tear down                                                     |
| 9  | Connected       | user `LoadSymbols`/swap variant    | —                                                    | Reloading       | (none yet)                                                                         |
| 10 | Connected       | `staleSignalDetected`              | strategy == `SymbolVersionAutoReload`                | Reloading       | bump reload-attempt counter, mark in-flight Updates `Stale=true`                   |
| 11 | Connected       | `staleSignalDetected`              | strategy == `SymbolVersionClose`                     | Closed          | fire `onDisconnect`, tear down                                                     |
| 12 | Connected       | `staleSignalDetected`              | strategy == `SymbolVersionIgnore`                    | Connected       | mark next Update for affected handle `Stale=true`, surface PLC error to caller     |
| 13 | Reloading       | reload OK                          | —                                                    | Connected       | bump `epoch`, swap cache.symbols, resubscribe notifications, fire `onReconnect`    |
| 14 | Reloading       | reload fail                        | reload-attempts exhausted (R-CACHE-013)              | Connected       | degrade to `SymbolVersionIgnore` semantics, log WARN, invoke version callback      |
| 15 | Reloading       | reload fail                        | strategy == `SymbolVersionClose`                     | Closed          | tear down                                                                          |
| 16 | Reloading       | `dropDetected`                     | `autoReconnect == true`                              | Disconnected    | abort reload, then continue with full reconnect                                    |
| 17 | Reloading       | user `Close`                       | —                                                    | Closed          | abort reload, tear down                                                            |
| 18 | Disconnected    | (immediate)                        | `autoReconnect == true`                              | Reconnecting    | (none)                                                                             |
| 19 | Disconnected    | (immediate)                        | `autoReconnect == false`                             | Closed          | tear down                                                                          |
| 20 | Disconnected    | user `Close`                       | —                                                    | Closed          | tear down                                                                          |
| 21 | Reconnecting    | dial+handshake+reload+resub OK     | —                                                    | Connected       | bump `epoch`, fire `onReconnect`                                                   |
| 22 | Reconnecting    | attempt fails                      | `attempts < max`                                     | Reconnecting    | apply backoff (self-loop)                                                          |
| 23 | Reconnecting    | attempt fails                      | `attempts >= max`                                    | Closed          | tear down                                                                          |
| 24 | Reconnecting    | user `Close`                       | —                                                    | Closed          | abort attempt, tear down                                                           |

Transitions not in the table are forbidden. Attempting one is a programming
error and SHALL panic in development builds (race-detector or `ADS_DEBUG=1`)
and log an ERROR + return `ErrInvalidState` in production builds.

## Invariants per state

These are the things tests assert about the system while it sits in each state.

### Constructed

- `transport == nil` (no socket).
- No goroutines running on this Connection.
- All counters at zero.
- Public API: only `Connect` and `Close` legal. Read/Write/notification calls
  return `ErrNotConnected`.

### Connecting

- `transport != nil` and dial in progress, OR dial done and listen+transmit
  spawning.
- `onDisconnect` SHALL NOT fire from this state.

### Connected

- `transport.connection != nil` and the listen + transmit goroutines are alive.
- `cache.lock` may be held briefly for inserts (on-demand `getSymbol`); never
  swaps cache.symbols (swap is a Reloading transition).
- Notification dispatch active.
- `epoch` is the value most recently bumped on entry.

### Reloading

- TCP still up; `transport.connection` unchanged.
- `cache.lock` may swap cache.symbols (only legal during Reloading or Reconnecting).
- Notifications: dispatch is paused for affected handles per R-NOT-017 step 1.
- Reload-attempt counter is being checked against `MaxSymbolVersionReloadAttempts`.

### Disconnected

- Listen goroutine has observed an error and exited.
- `transport.connection` is closed and zeroed.
- All in-flight requests in the request-mux SHALL fail with `ErrDisconnected`.
- `onDisconnect` HAS fired (exactly once per drop event, R-SES-008).

### Reconnecting

- A reconnect goroutine is performing dial / handshake / reload / resubscribe.
- New API calls block on `reconnectDone` channel (legacy semantic preserved).
- During the reload sub-phase, cache.symbols may be swapped.

### Closed

- All goroutines have exited.
- `transport.connection` is nil.
- Public API: every method returns `ErrClosed`.
- No transitions out (terminal).

## Counter unification: `epoch`

The current code carries two atomic counters with different bump triggers:

- `cache.generation` — bumped on every cache.symbols swap.
- `lifecycle.reconnectGeneration` — bumped on successful reconnect.

These exist for stranded-pointer detection in retry helpers (R-NOT-004,
`readMultipleSymbolsRetry`). They differ because reconnect with an empty cache
does not currently bump cache.generation.

**Unification rule**: bump `epoch atomic.Uint64` on every transition INTO
`Connected`. This is exactly the moment the user-facing world is "fresh
again." Drop both `cache.generation` and `reconnectGeneration` in favor of
`epoch`. To preserve R-CACHE-003's semantics for cache.symbols swap during
on-demand insert (which must NOT bump): on-demand `getSymbol` mutates
cache.symbols **in place** (no swap), and the FSM stays in Connected — `epoch`
does not bump. Only a swap-triggering transition (Reloading → Connected,
Reconnecting → Connected) bumps.

This collapses two counters into one and makes the bump rule mechanical:
"counter = number of times we've entered Connected."

## Mapping: pre-Phase-1 primitives → post-Phase-3 FSM

Historical reference. The reconnector struct has been renamed to
`sessionLifecycle` and folded into `session.go` (see commit `ffd0d23`);
`reconnector.go` no longer exists. The mapping below reflects the
state-of-the-world after Phases 1–3 landed.

| Pre-Phase-1 primitive                                | Post-Phase-3 equivalent                                  |
|------------------------------------------------------|----------------------------------------------------------|
| `lifecycle.closed atomic.Bool`                       | `state == SessionStateClosed`                            |
| `lifecycle.disconnected atomic.Bool`                 | `state == Disconnected \|\| Reconnecting` (FSM-derived); transport.disconnected (Phase 5.c) is the lower-level transport-down flag |
| `lifecycle.reconnecting atomic.Bool`                 | `state == SessionStateReconnecting`                      |
| `lifecycle.closedCh chan`                            | retained as `closedCh` for goroutine signaling           |
| `lifecycle.reconnectDone chan`                       | per-attempt channel inside Reconnect's defer             |
| `lifecycle.reconnectGeneration atomic.Uint64`        | folded into `sessionFSM.epoch`                           |
| `cache.generation atomic.Uint64`                     | folded into `sessionFSM.epoch`                           |
| `lifecycle.ctx`/`lifecycle.shutdown`                 | unchanged — goroutine-cancellation tool, not FSM state   |

`autoReconnect`, `maxReconnectAttempts`, `backoffConfig`,
`strictReconnect*` stay where they are — those are *configuration*, not state.

## Refactor sequencing — IMPLEMENTED

Status of each phase as of 2026-05-09:

1. **Add the FSM type without removing the flags** — DONE (commits
   `8bb7aca` + `3163467`). `SessionState` enum and `sessionFSM` helper
   shipped; transitions wired alongside the legacy flags.
2. **Switch readers** — DONE (Phase 2.a/2.b/2.c, commits `3da53be`
   `fde1329` `aea5f35` `05e9af9`). All call sites read from the FSM.
3. **Switch writers** — DONE (Phase 3, commit `86dc660`). Legacy
   `closed` and `reconnecting` flags removed.
4. **Unify counters** — DONE (Phase 4, commit `b48b0e0`).
   `cache.generation` and `reconnectGeneration` replaced by
   `sessionFSM.epoch` bumped on Connected entry + on cache-swap sites.
5. **Wire DP-1 strategies as FSM transitions** — PENDING. Implementation
   blocked on hardware tests (#31).

Phase 5 also covered the layered split (5.a/5.b/5.c): `Client` now owns
the raw RPC surface; `Session` owns cache + notifications + FSM; the
`disconnected` flag moved onto `transport`.

## Spec entries to add (after this doc is reviewed)

To `01-requirements.md` add a new module **R-FSM**:

- R-FSM-001: state field is the single source of truth.
- R-FSM-002: transitions occur only via the transition helper.
- R-FSM-003: transitions are atomic with respect to FSM mutex; readers may
  observe atomically-loaded state lock-free.
- R-FSM-004: forbidden transitions panic in dev / log+error in prod.
- R-FSM-005: `epoch` bumps exactly on entry to Connected.
- R-FSM-006: every spec invariant currently quoting a flag is rewritten to
  quote a state.

The existing R-SES-008 (`onDisconnect` exactly once) becomes a property of
the Connected → {Disconnected, Closed} transition: the helper fires the
callback exactly once because the transition is atomic.

The existing R-CACHE-003 (generation bumps on swap) becomes a property of
the Connected → Reloading → Connected and Reconnecting → Connected
transitions: those are the only swap-bearing transitions, and `epoch`
bumps on Connected entry.

## Open questions

1. **Lifecycle enum location.** `lifecycle.go` (new file) or in
   `connection.go`? Suggest new file to keep the FSM semantically isolated.
2. **Public exposure of state.** Should we expose `Connection.State()
   Lifecycle` publicly? Beckhoff's lib does not expose state. benthos-umh
   currently uses `IsDisconnected()`. Keep `IsDisconnected()` as a thin
   wrapper for backward compat, leave `State()` internal until a real
   consumer asks.
3. **Strict mode default.** Today `autoReconnect` defaults to true.
   `WithStrictReconnect` opts into a different escalation policy. After FSM,
   should the Connecting → Closed (autoReconnect=false) path remain reachable?
   Suggest yes — preserves the strict-socket variant for the hypothetical
   web-ADS-browser one-shot consumer (per architectural critique alternative B
   "additive future").
4. **Thread-safety of transition helper vs userland calls.** Concurrent
   `Read()` while `Reconnect` is in flight — currently legacy code blocks on
   `reconnectDone`. After FSM, blockers SHALL be the FSM transition helper
   itself (cond.Wait on state == Connected). No semantic change for users.
5. **Reload during user LoadSymbols.** Today user-driven `LoadSymbols` happens
   without any FSM annotation. Per transition #9, it becomes Connected →
   Reloading → Connected. Concurrent user reads block during Reloading —
   that's a small behavior change worth noting; verify benthos-umh does not
   call LoadSymbols mid-flight (per `08-usage-patterns.md` it does not).

## Acceptance criteria for this design doc

- All seven states defined with clear semantics.
- All transitions enumerated with guard, source event, and side effects.
- Every existing flag and counter has a named replacement.
- DP-1 strategies fit into the table without extra primitives.
- Open questions explicit.

When the user approves this doc, the next step is the detailed
refactor plan (one task per phase) feeding into `02-quality-constitution.md`'s
TDD protocol.
