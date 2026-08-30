# go-ads

A pure Go library for communicating with Beckhoff TwinCAT PLCs using the ADS (Automation Device Specification) protocol.

> **v2.2 breaking change**: every RPC method takes a `context.Context` as the first argument. `NewSession` accepts a typed `AMSEndpoint` plus options instead of 7 positional arguments. `Connect` takes `ctx` instead of a local-mode bool (use `WithLocalMode()`). `Symbol` is unexported (use `SymbolView`). `Update.Stale` is now `*StaleInfo`. See [CHANGELOG.md](CHANGELOG.md) for the full migration sketch.
>
> **v2.1 breaking change**: the previous `Connection` type has been renamed to `Session`, and the raw RPC surface has been split off into a separate `Client` type.

## Features

- **Connect with only an IP** — the target AmsNetId and AMS port are probed
  from the device when omitted (no route, no credentials needed for the probe).

- Connect to TwinCAT 2 and TwinCAT 3 PLCs over TCP
- Two-layer API: a Beckhoff-equivalent raw `Client` for one-shot consumers and a managed `Session` that adds caching, notification persistence, and auto-reconnect
- Read/write PLC symbols by name
- Batch read multiple symbols in a single round-trip (SumRead)
- Subscribe to symbol change notifications (single and batch)
- Automatic symbol table and datatype discovery
- Auto-reconnect with configurable backoff and notification re-subscribe
- Smart AMS route registration (probe-first, credential fallback)
- Stale handle detection across reconnects (epoch counter)
- Disconnect/reconnect event callbacks
- Symbol version change detection and refresh

## Install

```bash
go get github.com/RuneRoven/go-ads/v2
```

## Quick start

```go
package main

import (
	"context"
	"fmt"
	"time"

	ads "github.com/RuneRoven/go-ads/v2"
)

func main() {
	ctx := context.Background()

	target, _ := ads.NewAMSAddress("5.1.2.3.1.1", 851)
	sess, _ := ads.NewSession(ctx, ads.AMSEndpoint{IP: "192.168.1.100", Port: 48898, AMS: target},
		ads.WithRoute("my-route", "Administrator", "1"),
		ads.WithRequestTimeout(5*time.Second),
	)
	if err := sess.Connect(ctx); err != nil {
		panic(err)
	}
	defer sess.Close()

	// Symbols are resolved on-demand — no full discovery needed.
	value, _ := sess.ReadFromSymbol(ctx, "MAIN.myVar")
	fmt.Println("Value:", value)

	// Optional: load full symbol table for listing / struct access.
	sess.LoadSymbols(ctx)
	symbols, _ := sess.ListSymbols()
	fmt.Printf("Total symbols: %d\n", len(symbols))
}
```

## Target discovery — connecting without an AmsNetId

A wrong or absent NetID is the most common ADS misconfiguration, and the hardest
to recognise: the router accepts the TCP socket and then silently drops every
request, which looks nothing like an addressing mistake. So the address is
optional — omit `AMS` and the device is asked for it.

```go
// No AMS field at all: NetID and AMS port both come from the device.
sess, err := ads.NewSession(ctx, ads.AMSEndpoint{IP: "192.168.1.100"},
    ads.WithRoute("my-route", "Administrator", "1"),
)
```

The probe is the AMS router's identify service — UDP on the route-registration
port (48899), service id 1. It **registers nothing, needs no route and no
credentials**, and it answers before any route exists, which is what makes it
usable to bootstrap a connection rather than only to inspect one. Verified
against TwinCAT 2.10 (CX), TwinCAT 3.1.4024 (CX) and TwinCAT 3.1.4026 on TC/RTOS,
all three answering a request that carries a zero source NetID — so discovery
needs no local configuration either.

What you get, logged at Info so an operator learns the device's identity from one
line even when they supplied nothing:

```
INFO discovered target AMS address host=192.168.3.118 netID=5.66.133.203.1.1
     port=851 hostName=CX-4285CB twinCAT=3.1.4024
```

Cold bootstrap, measured end to end against a PLC with an empty route table and a
fresh boot (~1s):

```
discovered target AMS address netID=5.66.133.203.1.1 port=851 twinCAT=3.1.4024
route probe failed, registering route      <- no route yet, detected by trying
registering route localNetID=192.168.3.52.1.1 routeName=my-route
route registration successful
TCP reconnected after route registration
connected: symbol version 1
```

Two deliberate refusals:

- **The AMS port is inferred from the reported TwinCAT version, never guessed.**
  If a device reports no version, `NewSession` fails and tells you to set
  `AMS.Port` explicitly rather than planting 851 and addressing a runtime that may
  not exist. Multi-runtime projects (811, 852, …) must set it anyway — the
  inferred port is logged so that is visible.
- **The response is the identity of the router answering at that IP**, not "the
  NetID of the PLC behind it". On an embedded target (a CX, where router and
  runtime are the same device) those coincide. On an engineering PC or a gateway
  fronting other PLCs it is that machine's NetID and the PLC you want is an entry
  in its route table. Nothing in the response distinguishes the two, so nothing
  here pretends to.

Without credentials and without a route, `Connect` fails — and says why, in the
error itself rather than only in a log line:

```
transport dropped during connect to 192.168.3.118: ads: client transport closed
(a reset right after TCP connect means one of: no route is registered on the PLC
for our NetID (192.168.3.52.1.1), the target NetID (5.66.133.203.1.1) does not
exist on this PLC, the route credentials were rejected, or AMS port 851 addresses
no running runtime)
```

Use `WithTargetCheck` to verify a NetID you *did* supply against the device — see
[Connection options](#connection-options).

### Routes are matched by NetID, not by name

A route entry on the PLC authorises a **source AmsNetId** (and the IP it comes
from). The name is a PLC-side label, so it is needed only to *create* a route —
never to use one that already exists:

```go
// Already-provisioned device: no NetID, no route name, no credentials.
sess, err := ads.NewSession(ctx, ads.AMSEndpoint{IP: "192.168.1.100"})
```

Consequences worth knowing, all measured on TwinCAT 2.10 and TwinCAT 3.1.4024:

- **Passing a different route name does not create a second entry.** With a route
  already present for your NetID the probe succeeds and registration is skipped —
  logged as `route already exists on PLC, skipping registration`. Duplicate entries
  are one of the ways these devices go mute, so this matters more than it looks.
- **What must line up is your source NetID.** Without `WithLocalAMS` the library
  derives it from the host IP, so a pre-registered route has to have been created
  for *that* NetID. If it was not, the PLC resets the connection and the error names
  it: `no route is registered on the PLC for our NetID (…)`.
- **A single failed probe is not taken as proof a route is missing** — it is retried
  once before falling back to registration, because the first transport attempt
  after a PLC boot is unreliable (observed on TC2).

Cold bootstrap timings against an empty route table, for reference: ~1.0s on
TwinCAT 3.1.4024, ~1.2s on TwinCAT 2.10 (including the retry above).

### If UDP 48899 is blocked

The identify service and route registration share that port, so a firewall in front
of it removes both — while ordinary ADS traffic, which is TCP 48898 only, keeps
working. Measured behaviour:

| capability | UDP 48899 blocked |
|---|---|
| NetID / port discovery | unavailable — pass `AMS` explicitly |
| route registration | unavailable — pre-register the route on the PLC, or pass `WithSkipRouteRegistration()` |
| `WithTargetCheck` verification | skipped, never fatal (see below) |
| reads, writes, notifications | unaffected |

Discovery fails fast rather than handing back a session that would silently drop
every request — three attempts inside a 3s budget, then `NewSession` returns:

```
ads: NewSession: target AMS address incomplete and discovery failed (set remote.AMS
explicitly if the device does not answer the identify service): identify 192.0.2.1:
no answer after 3 attempts: ... i/o timeout
```

Verification is different on purpose: **an unanswered probe is never a failure, in
any `TargetCheck` mode.** A Windows TwinCAT host was observed serving ADS on TCP
with UDP 48899 firewalled off, and refusing those sessions over a check that could
not run would trade a real capability for a diagnostic. Only a definite mismatch is
reported. The skip is logged at Info, so silence never reads as "verified".

## Two layers

The library exposes two types. Pick the one that fits your consumer.

### Client — raw RPC (Beckhoff-equivalent)

`Client` is a thin wrapper around one TCP connection. Each method is a single ADS round-trip. No symbol cache, no notification persistence, no auto-reconnect. If the transport drops, every subsequent call returns `ErrTransportClosed` and the caller reconstructs a new `Client`.

Use this for one-shot consumers — CLI tools, web ADS browsers doing a quick probe, scripts that send a single command and exit.

```go
ctx := context.Background()

client, err := ads.Dial(
    "192.168.1.100", 48898,
    ads.AMSAddress{NetID: [6]byte{5, 1, 2, 3, 1, 1}, Port: 851},
    ads.AMSAddress{NetID: [6]byte{192, 168, 1, 50, 1, 1}, Port: 30000},
    5*time.Second,
    ads.WithClientLogger(slog.Default()),
)
if err != nil {
    panic(err)
}
defer client.Close()

info, _ := client.ReadDeviceInfo(ctx)
fmt.Printf("Device: %s\n", string(info.DeviceName[:]))

handle, _ := client.GetHandleByName(ctx, "MAIN.myVar")
defer client.ReleaseHandle(ctx, handle)

data, _ := client.Read(ctx, uint32(ads.GroupSymbolValueByHandle), handle, 4)
fmt.Printf("Value bytes: %v\n", data)
```

### Session — managed (long-running consumers)

`Session` wraps a `Client` and adds the value-add for long-running consumers: symbol cache, name-based read/write, notification persistence with auto-resubscribe, auto-reconnect with backoff, online-change handling, lifecycle callbacks. `Session` does NOT promote Client methods — call `sess.ReadFromSymbol(name)` for cache-aware access; raw consumers construct a separate `*Client`.

Use this for daemons, message brokers, or anything that should survive a network blip without manual intervention.

```go
ctx := context.Background()
target, _ := ads.NewAMSAddress("5.1.2.3.1.1", 851)
sess, _ := ads.NewSession(ctx, ads.AMSEndpoint{IP: "192.168.1.100", Port: 48898, AMS: target},
    ads.WithRoute("my-route", "Administrator", "1"),
    ads.WithAutoReconnect(true),
    ads.WithOnReconnect(func() { log.Println("back online") }),
)
sess.Connect(ctx)
defer sess.Close()

// Cache-aware read (resolves on-demand, then caches for the connection's lifetime).
value, _ := sess.ReadFromSymbol(ctx, "MAIN.myVar")

// Persistent subscription (resubscribes automatically after a reconnect).
ch := make(chan *ads.Update, 64)
sess.AddSymbolNotification(ctx, "MAIN.bCounter", 100*time.Millisecond, 100*time.Millisecond,
    ads.TransModeServerOnChange, ch)
for update := range ch {
    if update.Stale != nil {
        fmt.Println("STALE:", update.Stale.Reason, update.Variable, update.Value)
        continue
    }
    fmt.Println(update.Variable, update.Value)
}
```

## Example CLI

A ready-to-run example is included with two demo modes (Session-managed and
raw Client). Selection is interactive by default, or via `ADS_DEMO`:

```bash
cd examples/cli
ADS_PLC_IP=192.168.1.100 ADS_TARGET_AMS=5.1.2.3.1.1 \
ADS_SYMBOL_NAME=MAIN.bCounter ADS_DEMO=session go run .
```

See `examples/cli/README.md` for the full env-var reference.

## Connection options

All options are passed to `NewSession` and have sensible defaults:

```go
target, _ := ads.NewAMSAddress(netid, 851)
sess, _ := ads.NewSession(ctx, ads.AMSEndpoint{IP: ip, Port: 48898, AMS: target},
    // Route registration — probe first, register with credentials only if needed
    ads.WithRoute("my-route", "Administrator", "1"),

    // Custom logger (default: slog.Default())
    ads.WithLogger(myLogger),

    // Reconnect backoff strategy (default: 1s×3, 5s×3, 15s×4, then 30s cap)
    ads.WithBackoff(ads.BackoffConfig{
        InitialInterval: 1 * time.Second,
        InitialAttempts: 3,
        MidInterval:     5 * time.Second,
        MidAttempts:     3,
        SlowInterval:    15 * time.Second,
        SlowAttempts:    4,
        MaxInterval:     30 * time.Second,
    }),

    // Disable auto-reconnect (caller must call sess.Reconnect() manually)
    ads.WithAutoReconnect(false),

    // Event callbacks (run in goroutine, must not block)
    ads.WithOnDisconnect(func() { log.Println("disconnected") }),
    ads.WithOnReconnect(func() { log.Println("reconnected") }),

    // Fail reconnect if previously-resolved symbols are missing (e.g., after PLC online change)
    // maxAttempts=3 means retry 3 times before closing the connection
    ads.WithStrictReconnect(3),

    // Always register route with credentials (skip probe, for non-persistent route environments)
    ads.WithForceRouteRegistration(),

    // Override callback IP for Docker/VPN/NAT (default: derived from AMS NetID)
    ads.WithHostIP("192.168.1.50"),
)
```

| Option | Default | Description |
|--------|---------|-------------|
| `WithRoute(name, user, pass)` | No route | Register AMS route via UDP. Probes first, registers only if needed |
| `WithBackoff(cfg)` | 1s×3, 5s×3, 15s×4, 30s cap | Stepped reconnect backoff with configurable tiers |
| `WithAutoReconnect(bool)` | `true` | Auto-reconnect on TCP errors. When `false`, sets disconnected flag only — caller must call `Reconnect()` |
| `WithOnDisconnect(fn)` | None | Callback on disconnect detection |
| `WithOnReconnect(fn)` | None | Callback after successful reconnect |
| `WithStrictReconnect(n)` | Graceful skip (missing symbols warned + removed) | Fail if on-demand symbols missing after reconnect. `n` = max retry attempts before closing |
| `WithForceRouteRegistration()` | Probe first | Always register route with credentials (skip probe). Requires `WithRoute` |
| `WithHostIP(ip)` | Derived from AMS NetID | IP the PLC uses to reach this client (Docker/VPN/NAT). Requires `WithRoute` |
| `WithLogger(logger)` | `slog.Default()` | Custom structured logger |
| `WithSymbolVersionStrategy(s)` | `SymbolVersionAutoReload` | Online-change handling strategy (`AutoReload` / `Close` / `Ignore`) |
| `WithMaxSymbolVersionReloadAttempts(n)` | `3` | Cap reload attempts within the sliding window (AutoReload only) |
| `WithSymbolVersionReloadWindow(d)` | `60s` | Sliding window length for the reload-attempt cap |
| `WithOnSymbolVersionChanged(fn)` | None | Callback fired once per online-change detection (`reason` is one of the `Reason*` constants). Also fires with `ReasonHeartbeatSilent` under `WithHeartbeatRecovery(HeartbeatRecoveryObserve)` |
| `WithLocalAMS(addr)` | NetID auto-derived; Port random in 32768-49151 | Override source AMSAddress in outgoing ADS headers. The AMS port is a logical identifier inside the header, NOT the TCP source port (kernel-assigned) and NOT the TCP destination port (always 48898). Each Session randomizes by default so distinct Session instances present distinct AMS source identities to the PLC |
| `WithLocalBindIP(ip)` | Unset — OS picks via routing table | Pin the outbound TCP source IP. Used for multi-Session deployments on hosts with IP aliases. Invalid IP → Warn + nil (OS routing). See §Limitations for the multi-Session constraint |
| `WithSkipRouteRegistration()` | Off | Explicit opt-out from probe+AddRoute. Required when routes are managed externally (TC3 UI pre-registered, AmsRouterDaemon front-end). Equivalent to omitting `WithRoute` but explicit |
| `WithLocalMode()` | Off | Target the in-process TwinCAT runtime at 127.0.0.1 |
| `WithRequestTimeout(d)` | 5s | Per-request timeout for ADS commands and initial TCP dial |
| `WithNotificationHeartbeat(interval, missed)` | `2s`, `5` | Tune the internal cyclic subscription that detects subscriptions dying silently. `interval` is clamped to the ADS cycle-time limit (~400s); `missed` below 2 is raised to 2 |
| `WithoutNotificationHeartbeat()` | Heartbeat on | Opt out. Saves one PLC notification handle and one 1-byte sample per interval, at the cost of not noticing a runtime restart that leaves the TCP connection up |
| `WithNotificationSilenceTimeout(d)` | Derived from `missed` (10s) | State the tolerated silence as a duration instead of a tick count; `missed` is derived (rounded up, floored at 2). Last of this and `WithNotificationHeartbeat`'s `missed` wins |
| `WithHeartbeatRecovery(mode)` | `HeartbeatRecoveryImmediate` | What happens when the heartbeat goes silent. `Immediate` re-subscribes at once; `Confirm` waits for a second consecutive silent window before churning every handle; `Observe` reports (log + `ReasonHeartbeatSilent` callback) and leaves the rebuild to the consumer |
| `WithRuntimeStateWatch(d)` | `5s` | How often the runtime state (RUN/CONFIG) is polled on the system service port. **Was previously the heartbeat interval** — the two are independent as of this release |
| `WithoutRuntimeStateWatch()` | Watch on | Turn the state poll off. The gates on symbol and subscription calls then fall back to permitting, so a PLC in CONFIG fails the older, more obscure way |
| `WithAmsPeerListen(port)` | Off (fallback binds 48898 on demand) | Listen on `port` for a connection the PLC opens to US. Needed for devices that treat their route to this host as a peer route and answer only on their own connection |
| `WithoutAmsPeerFallback()` | Fallback on | Never bind the AMS port. Use on hosts where a local TwinCAT router owns 48898 |

### Combining options

All options are composable — no mutual exclusions. Some require others to have effect:

| Option | Requires | Notes |
|--------|----------|-------|
| `WithForceRouteRegistration()` | `WithRoute()` | No-op without route credentials |
| `WithHostIP()` | `WithRoute()` | Only affects route registration packet |
| `WithBackoff()` | — | Used in both auto and manual `Reconnect()` |
| `WithStrictReconnect()` | — | Only affects on-demand symbols (resolved via `GetSymbol` before reconnect) |
| `WithOnDisconnect()` / `WithOnReconnect()` | — | Fire regardless of auto/manual reconnect mode |
| `WithAutoReconnect(false)` | — | Backoff still applies when caller invokes `Reconnect()` manually |
| `WithSkipRouteRegistration()` | — | Overrides `WithRoute`. Skip wins. Useful when an options chain is built uniformly across Sessions and only some opt out |
| `WithLocalBindIP(ip)` | Host must have `ip` aliased on a local interface before `Connect` | OS returns "address not available" from Dial if alias missing |
| `WithAmsPeerListen(port)` | — | Also sets the port the automatic fallback uses. Mutually pointless with `WithoutAmsPeerFallback()` (the listener would never be used) |
| `WithNotificationHeartbeat()` | At least one subscription | The heartbeat is established alongside the first subscription and released with the last |

## Runtime state: CONFIG vs RUN

A TwinCAT system in configuration mode has no runtime port. Every request to it
comes back with AMS error `0x06` (target port not found) — measured on TC3.1.4024 —
while the **system service** on port 10000 stays up and answers `ADSState=15`
(CONFIG). So the state can be asked for, just not of the runtime.

The session polls it (at the heartbeat interval, one small request over the
connection it already has) and behaves accordingly:

| | Behaviour |
|---|---|
| `Connect` | **Succeeds.** The session is usable and waits; there is simply no runtime to talk to yet |
| `LoadSymbols`, `AddSymbolNotification(s)` | **Refuse**, with an error wrapping `ErrRuntimeNotRunning` that names the ADS state |
| `IsClosed()` | Stays `false` — nothing is wrong |
| Back in RUN | The poll notices and the same calls start working, with no reconnect and no rebuild |

```go
if _, err := sess.AddSymbolNotifications(ctx, configs, ch); errors.Is(err, ads.ErrRuntimeNotRunning) {
    // Not a failure to give up on: the PLC is in CONFIG. Try again later.
    state, _ := sess.RuntimeState(ctx) // ads.ADSStateConfig, ADSStateStop, ...
    log.Printf("PLC not running (ADS state %d), will retry", state)
}
```

Note the transition is usually CONFIG → STOP → RUN with a few seconds between, so
gate on "is it RUN", not "is it CONFIG": the runtime port exists during STOP but is
not executing.

The gate only ever fires on a positive reading. A device that does not serve the
system service port behaves exactly as before, and the poll gives up after five
consecutive failures.

## Diagnosing a dropped connection

A TCP drop has two very different meanings, and they used to look identical — both surfaced as
`ErrTransportClosed` with a hint naming the route. The session now classifies them by whether the
connection ever carried an AMS frame, and reports the verdict in the log for every drop, and as a
sentinel on the error `Connect` returns:

| Verdict | Sentinel | What it means | Where to look |
|---|---|---|---|
| Never served | `ErrRouteNotServed` | The connection was dropped without carrying a single frame | Route for our NetID, target NetID, credentials, AMS port, or another client on this host IP holding the router's single per-host TCP slot |
| Established drop | `ErrEstablishedDropped` | The connection had been delivering frames and was then dropped | Not the route — it was demonstrably being served. Eviction by another client on this IP, a runtime restart, or the network path |

```go
if err := sess.Connect(ctx); err != nil {
    switch {
    case errors.Is(err, ads.ErrEstablishedDropped):
        // the route worked a frame ago; do not go hunting route tables
    case errors.Is(err, ads.ErrRouteNotServed):
        // addressing or route problem
    }
}
```

**Scope of the sentinels:** they are attached to the error `Connect` returns when the transport
drops during a connect. A drop on an already-running session is handled by the reconnect
machinery and does not surface an error to the caller — RPCs issued in that window still fail
with `ErrTransportClosed`, and the verdict for that drop is in the log line below.

Both log lines carry `localPort`, `framesPrimary`, `framesPeer` and `uptime` at INFO, so a drop
can be correlated against a packet capture after the fact. Note `framesPeer`: on a device that
answers only on the connection it opens to us (see §Peer-route devices), the primary socket
carries no frames at all, so the verdict counts frames on either socket.

A connection that keeps dropping is throttled across reconnect cycles, not just within one:
anything that fails to stay up for 60s counts as a flap and escalates through the
`WithBackoff` tiers, up to `MaxInterval` (30s by default). Lower `MaxInterval` if stream
continuity during a flapping episode matters more than sparing the PLC's socket table.

## Peer-route devices

Some devices treat their route to this host as a *peer* route: they accept our TCP
connection, process our requests, and then send every response over a connection
**they** open to us on 48898. Measured on TC3.1.4026 (TC/RTOS); TC2 2.10 and
TC3.1.4024 answer on our connection instead. A client that cannot accept the
inbound connection sees every request time out, and Beckhoff's own Linux AdsLib
never listens, so it cannot talk to such a device at all.

Nothing needs configuring: when a Connect proves the device is otherwise silent,
the session binds the AMS port, discovers the device is answering there, and
carries on — announced at `WARN`, because a device in this state is worth an
operator's attention. `WithAmsPeerListen(port)` picks a different port;
`WithoutAmsPeerFallback()` refuses to bind at all, for hosts where a local TwinCAT
router owns 48898.

The fact is remembered per process, keyed by host and port: learning it costs a
probe timeout plus the route-activation budget (~18s), and every later session to
the same device skips straight to listening (~1s) and registers no route it does
not need.

## Notifications and backpressure

### Subscriptions that die silently

Restarting a TwinCAT runtime, or toggling CONFIG and back, kills every notification
a client holds **without anything observable happening**: measured on TC3.1.4024
across CONFIG → RUN with no program change, three runs including a fully passive
listener, the TCP connection survives, the symbol version is unchanged, ADS state
reads back identical, no error and no terminal sample arrives — and the
subscriptions never deliver again. 210 samples, then silence.

Silence alone proves nothing either, because an on-change subscription on a
constant symbol is legitimately silent forever. So the session keeps one internal
CYCLIC subscription on the symbol-version group: TwinCAT pushes those on a timer
regardless of change, which makes *its* silence conclusive. On that same transition
the beat and the caller's samples stop in the same second.

Missing five intervals (default 2s each) means the subscriptions are treated as
dead and re-established from the stored configs — no polling, no caller
involvement, and the PLC does the sending. Cost: one notification handle and one
byte per interval. Silence is counted in ticks of a monotonic ticker, so a
wall-clock step (an RTC-less box syncing NTP, a resumed VM) cannot fake it.

Recovery backs off as it fails, so a runtime left in CONFIG for an hour costs a
handful of attempts rather than one per interval, and the subscriptions stay on
file throughout.

Notification delivery is **non-blocking**: if the receiver's channel is full, the notification is dropped and a warning is logged. This prevents goroutine accumulation and ensures the receive pipeline is never stalled by a slow consumer.

**Sizing your channel buffer:**

```go
// Small buffer — fine for low-frequency or on-change notifications
ch := make(chan *ads.Update, 64)

// Large buffer — recommended for high-frequency cyclic notifications
// (e.g. 100 symbols × 10ms cycle = 10,000 notifications/sec)
ch := make(chan *ads.Update, 4096)
```

If you see `"notification dropped (channel full)"` warnings, either:
1. Increase the channel buffer size
2. Consume notifications faster (dedicated drain goroutine)
3. Reduce the notification cycle time on the PLC side

### Handle hygiene across reconnects and crashes

The library actively prevents PLC notification-handle accumulation
(Beckhoff/ADS #268) via three complementary strategies:

1. **Orphan-Delete.** When a notification arrives for a handle the
   Session no longer tracks (typically leaked by a prior process
   crash), the library asynchronously issues a best-effort Delete RPC
   for that handle. Throttled, sem-bounded, race-aware.
2. **Auto-reload pre-delete.** On `SymbolVersionAutoReload`, the
   library snapshots all current handles, wipes them locally, deletes
   them PLC-side, then reloads and re-subscribes. Prevents handle
   retention across online-change boundaries.
3. **Reconnect pre-delete.** On TCP reconnect, the library snapshots
   pre-reconnect handles, deletes them on the new transport, then
   re-subscribes with fresh handles. `0x715 DeviceClientUnknown` (PLC
   dropped client identity) is treated as cleanup-success.

No user action needed; the strategies fire automatically. Documented
in detail in [IMPLEMENTATION.md → Notification handle hygiene](IMPLEMENTATION.md#notification-handle-hygiene).

## Limitations

### One Session per source IP per target PLC

TwinCAT PLCs enforce a **single active TCP connection per source IP** on their ADS server (TCP/48898). This is a server-side constraint outside the library's control. Documented in [Beckhoff/ADS#49](https://github.com/Beckhoff/ADS/issues/49), [Beckhoff/ADS#72](https://github.com/Beckhoff/ADS/issues/72), [jisotalo/ads-client#47](https://github.com/jisotalo/ads-client/issues/47).

What this means concretely:

- ✅ **One process, one Session to one PLC** — supported, the normal case.
- ✅ **One process, multiple Sessions to *different* PLCs** — supported (each PLC sees one client).
- ❌ **Multiple Sessions to the *same* PLC from the same host IP** — second Session's TCP gets reset; if it sends `AddRoute` UDP, the first Session's existing TCP also gets closed by the PLC. This holds regardless of distinct AMS NetIDs, distinct AMS ports, distinct route names, or whether routes are pre-registered.
- ❌ **Multiple processes on the same host, each opening a Session to the same PLC** — same constraint; whichever process connects last wins.

Workarounds for multi-process / multi-Session-same-PLC:

1. **Distinct source IPs.** Deploy each process behind its own network namespace / container with bridge networking, so each has a unique IP from the PLC's perspective.
2. **`WithLocalBindIP(ip)`** if the host has multiple NICs or IP aliases — pin each Session to a distinct local IP. Requires admin-configured network setup.
3. **Local AMS router daemon** (Beckhoff's recommended pattern) — run [`Beckhoff.TwinCAT.Ads.TcpRouter`](https://www.nuget.org/packages/Beckhoff.TwinCAT.Ads.TcpRouter) or the open-source [AmsRouterDaemon](https://github.com/Beckhoff/ADS) on the host; client processes connect to `127.0.0.1:48898` and the daemon multiplexes a single connection to the PLC. The library does not bundle a router — it is a separate sidecar.

### Notification handle limits

Beckhoff documents ~550 notification handles per AMS port. The library does not enforce this cap; subscribing beyond it will fail at the PLC with `0x716 NoMoreHandles` ("no more notification handles available"). Coalesce subscriptions per process or split across distinct AMS ports if you need more.

## Process image I/O (experimental)

> **Warning:** Direct process image access bypasses the symbol table and writes raw bytes to I/O memory. Writing to the wrong offset can cause unexpected physical output changes (motors, valves, actuators). The PLC runtime may overwrite your changes on the next scan cycle. **For normal operation, use symbol-based access (`sess.ReadFromSymbol`/`sess.WriteToSymbol`).**

Process image methods live on `*Client` only — they are pure wire ops with no cache or notification dependency. Session users who need them construct a raw `*Client` via `Dial` alongside their `Session` (or use `Dial` exclusively if they never need cache-aware features).

```go
// Read 4 bytes from input image at byte offset 0
data, _ := client.ReadProcessInput(ctx, 0, 4)

// Read a single input bit (byte 2, bit 3)
val, _ := client.ReadProcessInputBit(ctx, 2, 3)

// Write to output image (use with extreme caution)
client.WriteProcessOutput(ctx, 10, []byte{0xFF})
client.WriteProcessOutputBit(ctx, 10, 0, true)

// Query input image size
size, _ := client.ReadProcessInputSize(ctx)
```

## Development

### Prerequisites

- Go 1.26+
- [golangci-lint](https://golangci-lint.run/docs/welcome/install/) v2.11+
- [gofumpt](https://github.com/mvdan/gofumpt) (`go install mvdan.cc/gofumpt@latest`)

### Before pushing

Run all checks locally — this mirrors what CI does on every PR:

```bash
make all        # format, lint, vet, test
```

Or run individual targets:

```bash
make fmt        # format code (gofumpt + goimports)
make lint       # run golangci-lint
make vet        # run go vet
make test       # run tests
make test-race  # run tests with race detector
make test-cover # run tests with coverage report
make build      # build all packages
```

### Network requirements

| Port | Protocol | Direction | Purpose | Configurable |
|------|----------|-----------|---------|--------------|
| 48898 | TCP | Client → PLC | ADS data (commands, responses, notifications) | Yes (`NewSession` port param) |
| 48899 | UDP | Client → PLC | AMS route registration (`WithRoute`) | No (Beckhoff fixed) |

Both ports must be open in firewalls between the client and PLC. If only TCP 48898 is open,
ADS works with pre-existing routes but `WithRoute` cannot register new ones.

### Docker / Container deployment

Only route credentials are needed — `WithHostIP` and `ADS_LOCAL_AMS` are optional:

```go
target, _ := ads.NewAMSAddress(targetAMS, 851)
sess, _ := ads.NewSession(ctx, ads.AMSEndpoint{IP: plcIP, Port: 48898, AMS: target},
    ads.WithRoute("my-route", "Administrator", "password"),
)
sess.Connect(ctx)
```

The PLC stores the UDP source IP (post-NAT) for the route, not the `computerName` from the
packet. Auto-derived NetID from the container IP works because ADS uses the existing TCP
connection for all communication including notifications.

> **Note:** Tested with TwinCAT 3 via Colima on macOS. More extensive testing across
> TwinCAT versions and container runtimes is ongoing. See `PROTOCOL.md` for details.

### Integration tests

Integration tests require a real Beckhoff PLC on the network. They are gated behind a build tag and skipped by default.

**Environment file format** (`.env.integration.XXX`):

```bash
## Required
ADS_PLC_IP=192.168.0.1          # PLC IP address
ADS_TARGET_AMS=5.0.0.1.1.1      # PLC AMS NetID
ADS_TARGET_PORT=851              # AMS port (851 for TC3, 801 for TC2)

## Optional — auto-derived if omitted
ADS_LOCAL_AMS=192.168.0.100.1.1 # Local AMS NetID (default: auto from local IP)
ADS_HOST_IP=192.168.0.100       # IP the PLC uses to reach us

## Optional — for auto-creating AMS route
ADS_ROUTE_USER=Administrator     # PLC admin username
ADS_ROUTE_PASS=1                 # PLC admin password

## Optional — test-specific symbol names
ADS_WRITE_BOOL=GVL_WriteTest.bWriteBool
ADS_READ_COUNTER=GVL_ProcessData.nMasterCycleCounter
```

**Running integration tests:**

```bash
# Source env and run all integration tests against a PLC
set -a && source .env.integration && set +a
go test -tags integration -v -timeout 60s

# Run a specific test
go test -tags integration -run TestIntegrationConnect -v -timeout 30s
```

**Symbol browse test** (`browse_test.go`) — connects to a PLC, loads all symbols slowly (chunked reads with delays to avoid disrupting real-time tasks), and writes a `.var` file documenting every symbol:

```bash
set -a && source .env.integration && set +a
go test -tags integration -run TestBrowseAllSymbols -v -timeout 60s
# → writes plc_192_168_0_1.var (filename derived from ADS_PLC_IP)
```

The `.var` files list all symbols with name, datatype, size, index group, offset, and comment.

### CI

CI runs automatically on pull requests to `main` with 4 parallel jobs: lint, test, test-race, and build. All must pass before merging.

## TODO

- **Enum string resolution**: Add an option to return enum constant names (e.g. `"RUNNING"`) instead of numeric values (e.g. `"2"`). Requires parsing enum constant values from the datatype table's extra data. Only possible for TC3 non-strict enums; TC3 strict enums and TC2 do not expose constant names in the datatype table.
- **TCP-based route registration**: Investigate whether AMS routes can be created via ADS system service commands over the existing TCP connection (AMS port 10000) instead of the UDP protocol (port 48899). This would eliminate the UDP 48899 firewall requirement and simplify deployment in locked-down networks. The Beckhoff `TcAmsRemoteMgr` service handles UDP route requests — a TCP equivalent may exist via the ADS system service but is unconfirmed.
- **In-process AMS router subpackage (`router/`)**: Embeddable Go AMS router that lets a single process host multiple `Session`s targeting the same PLC, and that doubles as a standalone daemon (`cmd/ads-router/`) front-end for multi-process deployments. Bypasses the TwinCAT 1-TCP-per-source-IP constraint (see §Limitations) without requiring Beckhoff's .NET `Beckhoff.TwinCAT.Ads.TcpRouter`. Lib internals already prepped: `AMSHeader` / `ParseAMSHeader` / `EncodeAMSHeader` wire codec exported, `NotificationHandler` callback type exported, `WithSkipRouteRegistration` option for router-fronted Sessions, `WithLocalBindIP` for multi-NIC host setups. Passthrough mode planned for v2.3.0 (no notification coalescing — each client gets its own PLC handle, matching Beckhoff's official TcpRouter behavior).

## License

MIT — see [LICENSE](LICENSE) for details.

Original ADS protocol implementation based on [go-native-ads](https://gitlab.com/xilix-systems-llc/go-native-ads) by Bob Klosinski.
