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

- v2.2.1 added orphan-Delete (Fix 1), auto-reload handle cleanup
  (Fix 2), reconnect post-dial Delete (Fix 3), random AMS port (Fix 4).
- v2.2.2 added route-probe-retry, ondrop suppression during Connect,
  0x715 ClientUnknown classified as cleanup-success, exported
  `AMSHeader`/`NotificationHandler`/`WithLocalBindIP`/`WithSkipRouteRegistration`,
  and route-name-per-source-IP test convention.
