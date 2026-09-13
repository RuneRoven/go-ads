# Findings — notification recovery and connect retries (2026-09-13)

Measured against the office PLCs over a tailnet: `10.13.37.107` (TC3 4024, NetID
`5.66.133.197.1.1`, port 851, hostname `CX-4285C5`) and `10.13.37.217` (TC2 2.10,
port 801). Client source address `100.97.160.53`. Hardware probes run from a
container; the umh-core bridge was stopped for any run that needed the PLC's
single per-host TCP slot.

These are the field observations behind the code. The code carries the decision,
not the evidence — look here for why a number is what it is.

## 1. A green, beating session that delivers nothing

`.107` sat healthy and silent: the heartbeat arrived every 2 s, no errors, no
data. Measured with `ss -tin`: 7 data segments in 10 s against 203 on a working
bridge — the beat and nothing else.

Mechanism: silence detection released every handle, the 41-symbol re-subscribe
failed, and the heartbeat re-established **on its own** because it is one small
request where a re-subscribe is a batch. With the beat back, silence never
recurred, so recovery was never triggered again. The session held 1 subscription
where it should have held 41 and looked healthy.

Detection: compare what a healthy session holds against what it holds now. The
beat proves the link, not the subscription set.

## 2. The baseline must be a high-water mark

The obvious baseline — store the count on every commit — is wrong. A partial
re-subscribe then redefines healthy as the smaller set it just achieved, so the
symbols it failed to restore leave no gap and nothing retries them.

Equally wrong is `len(pending)`: pending is a superset by design and outlives a
handle, so a symbol the PLC permanently refuses makes want != have for the life
of the session and churns recovery forever. Verified on hardware: 40 real symbols
plus 5 that do not exist settles at 40/40 and stays there over 549 samples.

Lowering is therefore deliberate, and has exactly three causes: the caller tears
a subscription down, the PLC no longer has the symbol, or a config is abandoned
after `resubscribeMaxAttempts`.

## 3. Detecting a dead transport by frames, not by errors

Counting classified errors was wrong twice: a PLC answering `1793` (device not
ready) is a refusal, not a dead link, and escalating on it tore down a working
transport. What separates the two is whether **any** frame arrives on either
socket. If the frame counter has not moved across consecutive silent windows, the
transport is gone and re-subscribing into it cannot help.

Measured on a cable pull: outage detection went from 107 s to 30 s.

TCP keepalive does not cover this. Linux arms the keepalive timer only on an
*idle* socket; any unacked data in the send queue switches it to the
retransmission timer bounded by `tcp_retries2` (~15 min). Confirmed by reading
the timer field in `/proc/net/tcp`: `keepalive` flipped to `retransmit` the
instant `txq > 0`, with the socket still alive at 85 s. Our own recovery traffic
is what suppresses keepalive, so it can never be the detector here.

## 4. The gap check must not run on a dead beat

With the link severed for 90 s (netem `loss 100%`, 40 symbols):

| | gap branch above the silence check | gated on a live beat |
|---|---|---|
| gap check fired while beats were frozen | 2x | 0x |
| silence -> "transport is dead" | 78 s | 60 s |
| handles across the outage | 40 -> 0 -> 40 | 40 -> 0 -> 40 |

Counting the gap and acting on it are separate decisions. The ticker runs at the
beat's own period, so gating the **count** on "this tick saw a beat" lets
ordinary jitter reset it and the check never reaches its threshold — with the
default 5 allowed misses it could never climb past 1.

## 5. `.107` fails its first connect after a restart, ~33-50% of the time

Reproduced 3 of 6 restarts. The failing path:

```
route probe failed at transport layer, retrying once before AddRoute  delay=500ms
route probe failed, registering route
route registration got no usable answer in 6s over 3 attempts: read udp4 ...:48899: i/o timeout
```

...and ~30 s later the same probe succeeds with `route already exists on PLC`.
**The route was never missing.**

Two hypotheses were raised and killed:

- *TwinCAT's single per-host TCP slot.* Killed by an identify probe from an
  unrelated socket that never held a slot: it went deaf for 8 s. A slot conflict
  cannot silence UDP 48899.
- *The router goes deaf.* Killed by the next round: identify **answered**
  immediately while `AddRoute` on the same port got nothing for its full 6 s
  budget. Both states are real and they are not the same failure.

What holds in both samples: the route exists, and only the TCP probe cannot
succeed for a while. Registering is never the right answer — it sends a request
for a route that is already there.

Fix: retry the probe for a grace instead of registering. A router answering
nothing at all is reported as retryable (`ErrRouterUnresponsive`) rather than
treated as a missing route.

Measured, 6 restarts each way:

| | before | after |
|---|---|---|
| rounds hitting it | 2 of 6 | 3 of 6 |
| connect failures | 1 each | 0 |
| doomed registrations | 2 each | 0 |
| time to 41/41 | 51 s | 24 s |

A hit now resolves in ~11 s over 7 probe retries, which is the first direct
measurement of that window — previously it was only bounded between 1.6 s and
36 s.

## 6. Log levels and rates

**Every failed reconnect attempt logs at Error.** Reporting only the first and
the rest at Debug was an attempt to stop a consumer's rolling error window from
advancing for the whole outage. It backfires: once the window passes that single
line the component reads as healthy while nothing flows, and the only thing still
printing is `reconnect backoff` at Info, which announces a retry but never says
what failed. A rolling window is correct while the link is genuinely down.

Context: umh-core's `BenthosLogWindow` is 10 minutes and holds a component out of
active on any error inside it. Measured twice, the tail is exactly 10 minutes
from the **last** error — which is the line written seconds before recovery. A
2-minute outage therefore costs 10 minutes of degraded state. That tail is
umh-core policy; nothing this library does shortens it.

**Orphan handle logs are rate-limited, not episode-based.** A PLC holding a
previous session's subscriptions pushes one sample per cycle per handle —
measured at 527 in one log file — and one delete per handle in a burst, measured
at 39 Info lines. An "episode" counter reset by the next owned sample does not
work: after a reconnect orphans interleave with healthy samples, so the episode
ends immediately and every orphan reports again. Reported once per 30 s with a
count instead.

## 7. Running the hardware probes

The probes live in `zz_*_integration_test.go` (gitignored, opt-in via
`ADS_ZZ_PROBES=1`). From a container, behind NAT, two overrides are mandatory or
the PLC accepts the TCP connection and then answers **nothing** — not
`GetSymbolVersion`, not `ReadState`:

```
ADS_SOURCE_AMS=100.97.160.53.1.1   # WithLocalAMS - the source NetID
ADS_HOST_IP=100.97.160.53          # WithHostIP  - the callback address
```

`WithHostIP` alone is not enough: it sets only the callback address, while the
source NetID is still derived from the container's own address and matches no
route. Both must name the post-NAT address the PLC actually sees.

Array symbols are skipped by the probes: with only the symbol list loaded (no
datatype table) an array has no children, so parse falls back to its element type
while keeping the whole array's length and logs one error per sample. That needs
`LoadSymbols`, which is more than the probes require.
