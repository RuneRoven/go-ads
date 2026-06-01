# Archived specifications

These behavioral specifications drove the v2.0 → v2.1 → v2.2 redesign of the
go-ads library (FSM, lock ordering, online-change handling, sum-fallback,
notification lifecycle). They captured requirements, review protocols, and
audit trails at design time.

**Status: historical reference, not maintained.**

After v2.2 shipped, the code became the source of truth. Cross-cutting
design documentation now lives in [`IMPLEMENTATION.md`](../../../IMPLEMENTATION.md);
wire-protocol details in [`PROTOCOL.md`](../../../PROTOCOL.md); per-symbol
contracts in godoc. New behavior is documented inline + in commit
messages; ongoing design discussions happen in commit history and PRs.

Reading these for context on **why** v2.x looks the way it does is useful.
Treating them as binding requirements for new code is **not** — they may
have drifted from current behavior.

Drift known since archive (non-exhaustive):

- v2.2.0 release (post-v2.1.0) added:
  - Notification-handle hygiene: orphan-Delete on unknown-handle
    samples, reconnect snapshot+best-effort-Delete, auto-reload
    pre-delete (the three strategies in IMPLEMENTATION.md
    §Notification handle hygiene).
  - Source-AMS-port randomisation in IANA dynamic range (32768-49151)
    to avoid stale-slot collisions on PLC.
  - Smart route registration: probe-first, single transport-RST
    retry, ondrop suppression during Connect/Reconnect.
  - `0x715 DeviceClientUnknown` classified as cleanup-success in
    `isBestEffortDeleteSuccess`.
  - New exports for router-prep: `AMSHeader` + codec,
    `NotificationHandler` callback type, `WithLocalBindIP`,
    `WithSkipRouteRegistration`.
  - Test convention: route name per source IP
    (`go-ads-{ip-with-dashes}`) to avoid PLC duplicate-name lookup
    ambiguity.
