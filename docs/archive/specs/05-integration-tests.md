# Integration Test Specifications

End-to-end protocol grounded in use cases. Each test maps to a user outcome AND a requirement. Hardware-only — runs against real Beckhoff PLCs.

## Hardware

| Tag | IP | NetID | TwinCAT | Port | Env file |
|-----|----|-------|---------|------|----------|
| TC2 | 192.168.3.70 | 5.3.69.134.1.1 | 2.x | 801 | `.env.integration.70` |
| TC3 | 192.168.3.224 | 5.154.236.19.1.1 | 3.x | 851 | `.env.integration.224` |

Both PLCs SHALL host the test PLC programs at the symbol paths used in the `.env.integration.*` files (`GVL_WriteTest.*`, `PRG_Diagnostics.*`, `PRG_Machine.*`, etc.). PLC programs are not in this repo (Beckhoff TwinCAT projects).

## Run protocol

```bash
# TC3
set -a && source .env.integration.224 && set +a && \
  sudo -E go test -tags integration \
    -run '<TEST_PATTERN>' \
    -v -count=1 -timeout 120s ./...

# TC2
set -a && source .env.integration.70 && set +a && \
  sudo -E go test -tags integration \
    -run '<TEST_PATTERN>' \
    -v -count=1 -timeout 120s ./...
```

`sudo` is required on macOS for Local Network privacy permission (UDP route registration); on Linux it's not required after firewall rules permit UDP/48899.

## Smoke set

The minimum that gates every release. Six tests, run on both PLCs (12 invocations).

| Test ID | Test name | Validates | User outcome |
|---------|-----------|-----------|--------------|
| T-I-001 | `TestIntegrationConnect` | R-SES-002, R-ROUTE-004 | "I can connect to the PLC and complete a handshake" |
| T-I-006 | `TestIntegrationReadSymbol` | R-CMD-002, R-CACHE-007, R-PARSE-001 | "I can read the current value of any named symbol" |
| T-I-007 | `TestIntegrationWriteAndConfirm` | R-CMD-003, R-PARSE-003, R-PARSE-004 | "I can write a value to a symbol and read it back" (table-driven across 17 ADS data types) |
| T-I-008 | `TestIntegrationNotification` | R-NOT-001, R-NOT-006, R-NOT-008, R-NOT-012 | "I can subscribe to a symbol and receive value updates" |
| T-I-025 | `TestIntegrationReconnect` | R-RECON-001, R-RECON-006, R-NOT-014 | "If the connection drops, the library reconnects and re-subscribes" |
| T-I-008b | `TestIntegrationBatchNotification` | R-NOT-009, R-SUM-003 | "I can subscribe to many symbols in one round-trip" |

A release MUST have all 12 invocations green within 24h of tagging.

## Full integration set (canonical)

Beyond the smoke set, the full integration suite covers requirements that need real PLC behavior to verify (handle reuse semantics, online-change scenarios, sum command fallbacks, ContextMask presence per PLC version).

### Connect / route

- **T-I-001** `TestIntegrationConnect` — handshake completes; on TC2 + TC3.
- **T-I-002** `TestIntegrationConnect_RouteAlreadyExists` — second Connect skips route registration. R-ROUTE-004.
- **T-I-039** `TestIntegrationRouteRegistration` — fresh route from scratch (delete via TwinCAT manager, reconnect). R-ROUTE-001.
- **T-I-040** `TestIntegrationRouteForceRegistration` — `WithForceRouteRegistration()` always registers. R-ROUTE-005.

### Read / Write

- **T-I-006** `TestIntegrationReadSymbol` — read a symbol whose value is known on the PLC.
- **T-I-007** `TestIntegrationWriteAndConfirm` — table-driven write+read for 17 data types: BOOL, SINT, INT, DINT, USINT, UINT, UDINT, REAL, LREAL, STRING, BYTE, WORD, DWORD, TIME, DATE, DT, TOD.
- **T-I-028** `TestIntegrationDirectGroupRead` — Read by group/offset, not by symbol.
- **T-I-016** `TestIntegrationStringLengthSemantics` — TC2 vs TC3 STRING length conventions (TC2 includes null terminator in Length per R-SYM-003).
- **T-I-017** `TestIntegrationBaseTypeMapping` — verify Symbol.BaseType matches PLC declared type.

### Notifications

- **T-I-008** `TestIntegrationNotification` — single subscribe, deliver an update, delete.
- **T-I-014** `TestIntegrationOnDemandNotification` — subscribe a symbol that wasn't in full discovery (lazy resolve via getSymbol).
- **T-I-007b** `TestIntegrationSubscribeUnsubscribe` — repeated subscribe-then-unsubscribe stress.
- **T-I-008c** `TestIntegrationCloseReleasesNotificationHandles` — Close cleans up PLC-side handles (verifiable via TwinCAT diagnostic).

### Reconnect

- **T-I-025** `TestIntegrationReconnect` — simulated TCP drop (`conn.Close()` on the underlying socket), reconnect succeeds, notification fires post-reconnect.
- **T-I-009** `TestIntegrationReconnectDuringBatchRead` — reconnect mid-SumRead; retry uses fresh handles.
- **T-I-024** `TestIntegrationStaleHandleAfterReconnect` — pre-disconnect handle becomes invalid; library re-resolves via reconnectGeneration check.
- **T-I-021** `TestIntegrationAutoReconnect` — `WithAutoReconnect(true)` (default).
- **T-I-022** `TestIntegrationManualReconnect` — `WithAutoReconnect(false)` requires `Reconnect()` call.
- **T-I-023** `TestIntegrationMaxReconnectAttempts` — N=2 caps total attempts.
- **T-I-026** `TestIntegrationStrictReconnect` — `WithStrictReconnect(0)`: Reconnect fails on missing on-demand symbol.

### Sum commands

- **T-I-033** `TestIntegrationSumReadEx2` — TC3 uses 0xF084.
- **T-I-034** `TestIntegrationSumReadEx` — TC2 uses 0xF083.
- **T-I-035** `TestIntegrationSumReadIndividualFallback` — force individual via `SumReadCmdStore(1)`.
- **T-I-036** `TestIntegrationSumWrite` — batch write across mixed types.
- **T-I-037** `TestIntegrationSumNotificationStateIndependent` — TC2 fallback for both Add+Delete; TC3 native; verifies independent state per opcode (R-SUM-003).
- **T-I-038** `TestIntegrationSumBatchN500` — large batch (Beckhoff soft limit).

### Discovery / browse

- **T-I-010** `TestIntegrationLoadSymbols` — full discovery, list symbols, count check.
- **T-I-011** `TestIntegrationLoadSymbolsSlow` — chunked download, custom chunk size.
- **T-I-012** `TestIntegrationLoadSymbolListBrowseMode` — list-only without datatypes.
- **T-I-018** `TestIntegrationListSymbols` — after full discovery.
- **T-I-019** `TestIntegrationGetSymbol` — on-demand single resolve.
- **T-I-031** `TestIntegrationReadWrite` — Cmd 9 directly via `WriteRead`.

### Online change (TC3 only)

- **T-I-006** ` TestIntegrationOnlineChangeMidNotification` (NEW; aspirational) — operator triggers online change while a notification is active. Validates R-NOT-005, R-CACHE-003, R-CACHE-004.

### Performance & load (NEW; aspirational)

- **T-I-020** `TestIntegrationHighRateNotifications` — 1000/s for 60s; verify worker pool handles without goroutine leak (per R-TX-006).
- **T-I-038b** `TestIntegrationLargeSymbolTable` — 50k symbols; ListSymbols memory + walk time bounded.

## Use-case mapping

Each test is grounded in a user outcome. The mapping is the contract: if the test passes but the user outcome doesn't materialize on real-world deployments, the test is wrong.

| User outcome | Tests |
|--------------|-------|
| "Read PLC values reliably from Go" | T-I-001, T-I-006, T-I-028, T-I-016 |
| "Write PLC values reliably with type safety" | T-I-007 (17 data types), T-I-031 |
| "Subscribe to value changes and receive low-latency updates" | T-I-008, T-I-014 |
| "Survive cable unplug / power blip without losing subscriptions" | T-I-025, T-I-009, T-I-024 |
| "Operate against TC2 and TC3 transparently" | every test runs on both |
| "Don't leak handles or goroutines under sustained load" | T-I-008c, T-I-020 (aspirational) |
| "Discover symbols and browse the variable tree" | T-I-010, T-I-011, T-I-012, T-I-018 |
| "Batch operations efficiently" | T-I-033..T-I-037 |
| "Recover gracefully from PLC online changes" | T-I-006 (TC3 online-change), T-I-026 |
| "Don't expose route credentials in logs" | covered by T-U-1101 (unit) |

## Hardware test runner

`docker/run-tests.sh` exists for containerized hardware tests. The release job runs:

```bash
docker/run-tests.sh tc2 .env.integration.70
docker/run-tests.sh tc3 .env.integration.224
```

Each invocation runs the smoke set + full integration set against the named PLC.

## Failure triage protocol

When a hardware test fails:

1. **Reproduce locally** with verbose output: `go test -tags integration -run '<TestName>' -v -count=1`.
2. **Categorize**:
   - PLC-side issue (program not deployed, network unreachable) — fix the PLC.
   - Library bug — file under B-NNN, reference the requirement violated.
   - Test bug — fix the test.
3. **Bisect**: if the test passed previously, `git bisect` to find the introducing commit.
4. **Capture diagnostic**: PROTOCOL.md decode of the failing wire packet (use `WithLogger` at LevelTrace).
5. **Add regression test** under `T-U-NNN` if the failure mode can be reproduced synthetically (preferred); promote the integration test to a canonical case.

## Aspirational tests not yet implemented

These exist in the spec but not in code. Filing as gap-bugs in `07-bug-report.md`:

- T-I-020 `TestIntegrationHighRateNotifications` — load test
- T-I-038b `TestIntegrationLargeSymbolTable` — 50k symbol stress
- T-I-006 (TC3) `TestIntegrationOnlineChangeMidNotification` — requires manual TwinCAT operator action; could be automated via TwinCAT Build & Activate API

These are NOT release blockers but are tracked as test gaps.
