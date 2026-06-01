# go-ads Test Suite Audit

Audit date: 2026-05-09 (initial), refreshed 2026-05-09 after Tier 1–3
remediation + scriptable PLC stub server. Scope: every `*_test.go`
file in the repo (excluding `integration_test.go` and
`testhelpers_test.go`, which are skimmed and treated as inventory only).
Compares actual tests to `specs/01-requirements.md` (R-XXX-NNN), the
test-ID scheme in `specs/04-functional-tests.md`, and the integration
plan in `specs/05-integration-tests.md`.

> Status: Tier 1 (delete tautology + strengthen R-CACHE-004) DONE.
> Tier 2 (rewrite mimic tests + spec backfill R-SYM-007/008, R-SUM-008)
> DONE. Tier 3 (fill missing-test gaps for R-CL/R-SES/R-CACHE/R-VIEW/
> R-RECON/R-NOT) DONE incl. follow-on scriptable-server commit
> `34c4de3` that unblocked the 6 t.Skip TODOs (R-NOT-004, R-NOT-003,
> R-NOT-008, R-NOT-013, R-NOT-015 partial, R-CACHE-007). Section 3
> entries that are now covered are marked **RESOLVED**.

Working assumptions:

- R-CON-* renamed to R-SES-* on 2026-05-08 — both are equivalent.
- Phase 5 split: thin `Client` (raw RPC) + `Session` wraps it (cache, FSM, reconnect, notifs).
- `NewConnection` renamed to `NewSession` (no `ctx` parameter post-rename).

---

## Section 1 — Traceability matrix

One row per test function. Test files in alphabetical order. `NO-SPEC` means the test doesn't trace to any R-XXX-NNN; flagged for backfill or deletion in Sections 2/4.

### `client_test.go`

| Test func | Validates | Notes |
|---|---|---|
| TestClient_DialClose | R-CL-001 | Dial+Close roundtrip, basic transport struct init. Stub TCP server. Adequate. |
| TestClient_DoubleClose | R-CL-001 | Idempotent Close. Adequate. |
| TestClient_DialFailsWhenServerUnreachable | R-CL-001 (negative path) / NO-SPEC strict | Asserts wrapped dial-error string. Borderline implementation-mimic on the error string format. |
| TestClient_OptionsApplied | R-CL-007 (handler install), partial R-SES-006 mapping for ClientOption | Verifies WithClientRequestTimeout, WithNotificationHandler, custom ClientOption. Mid-quality. |
| TestClient_TransportClosedSentinel | R-CL-003 | Tests sentinel matchability via `errors.Is`. Includes a dummy `errWrap` helper — partial coverage; no test that a closed Client's method returns ErrTransportClosed. |
| TestListen_TwoSequentialPackets | R-TX-002 | Split-packet listen via net.Pipe. Sends only system packets (System=1) routed to systemResponse. |
| TestListen_OversizePacketTriggersReconnect | R-TX-003 | 8 MiB > 4 MiB cap → listen exits without panic. |
| TestIsRouteHintErr_EOF | NO-SPEC | Tests `isLikelyMissingRoute` heuristic — no R covers route-hint detection. Low risk; supports route-probe error classification. |
| TestIsRouteHintErr_ECONNRESET | NO-SPEC | Same heuristic, ECONNRESET branch. |
| TestEncodePacket | R-CMD-002 (header layout, indirectly), R-TX-004 | Verifies AMS header byte-positions match wire format. Implementation-mimic risk: re-asserts header byte offsets. |
| TestEncodePacket_EmptyData | R-CMD-001 (indirectly) | Empty payload produces 38-byte packet. Same shape as TestEncodePacket. |
| TestEncodePacket_AllCommands | NO-SPEC strict | Loops 8 CommandIDs and asserts the cmd field is encoded. Coverage-shape only — would not fail on a wrong InvokeID, payload, or state field. |
| TestHandleReceive_RoutesToCorrectChannel | R-TX-004 | Per-invoke-ID multiplexing happy path. |
| TestHandleReceive_UnknownInvokeID | R-TX-004 | Unknown InvokeID does not panic. Should also assert log was emitted at Error/Warn (it does not). |
| TestHandleReceive_TooShort | R-CMD-007 | Truncated header rejected. Adequate. |
| TestHexAttr | NO-SPEC | Smoke test for the `hexAttr` slog helper. |
| TestHexAttr_Empty | NO-SPEC | Same helper, empty case. |

### `cmd_notification_test.go`

| Test func | Validates | Notes |
|---|---|---|
| TestDeliverNotification_ClosedChannelDoesNotPanic | R-NOT-006 | Recover-on-closed-channel path. Adequate. |
| TestDeliverNotification_DeliversOnOpenChannel | R-NOT-006 | Happy path; B-grade. |
| TestDeliverNotification_DropsWhenChannelFull | R-NOT-006 | Non-blocking drop; B-grade. |
| TestDeviceNotification_SingleSample | R-NOT-005, R-NOT-012 | Re-resolve + timestamp conversion. Adequate (A-grade for R-NOT-012). |
| TestDeviceNotification_UnknownHandle | R-NOT-007 (partial) | No-error / no-panic path. Does not assert log level distinction. |
| TestDeviceNotification_UnknownHandleDuringClose | R-NOT-007 | Asserts Debug level during close — strong test. |
| TestDeviceNotification_UnknownHandleNormalCondition | R-NOT-007 | Asserts Warn level outside close window. |
| TestDeviceNotification_MultipleStampsAndSamples | R-NOT-005 (partial) | Multi-stamp/multi-sample dispatch ordering. Mid quality. |
| TestDeviceNotification_EmptyPacket | R-CMD-007 | Truncated packet error. |
| TestDeviceNotification_ZeroStamps | NO-SPEC | Edge case: header valid, 0 stamps. No-error contract. |
| TestDeviceNotification_SampleSizeExceedsData | R-CMD-007 (sum-up) | Defensive sample-size check. |
| TestDeviceNotification_BoolType | R-PARSE-007 (BOOL) | Per-type parse via notification path. |
| TestDeviceNotification_StringType | R-PARSE-005 + R-PARSE-007 (STRING) | NUL-stop semantics through notification dispatch. |
| TestWindowsFiletimeConversion | R-NOT-012 | **MIMIC RISK**: test reverses the same formula in production (`int64(filetime)/windowsTick - secToUnixEpoch`). See Section 2. |

### `cmd_sum_test.go`

| Test func | Validates | Notes |
|---|---|---|
| TestParseSumReadResponse_LengthOverflow | R-SUM-006 | Length=0xFFFFFFFE → InvalidSize. A-grade. |
| TestParseSumReadResponse_TruncationLogsError | R-SUM-006 | Cascade-mark remaining items, log emitted. A-grade. |
| TestParseSumReadResponse_PerItemOversize | R-SUM-006 | Per-item oversize defense. A-grade. |
| TestBestEffortDeleteNotifications_Empty | R-NOT-015 | Maps to T-U-117 in spec. |
| TestSumReadOverflowGuard | R-SUM-006 | **MIMIC + TAUTOLOGY**: re-implements the addition `n*8 + total` in the test body, asserts `total > MaxUint32`. Doesn't touch any production function — pure arithmetic. See Section 2. |
| TestIsSumCommandUnsupportedError | R-SUM-001/R-SUM-002 (partial) | Tests the predicate that gates fallback. B-grade. |
| TestSumProbeStateTransitions | R-SUM-003 | Maps to T-U-800 — independent CAS for sumWriteState / sumAddNotifState / sumDeleteNotifState. A-grade. |
| TestSumProbeStateConcurrent | R-SUM-003 / R-LOCK-003 | Race-detector helper; 100 concurrent CAS attempts → exactly one winner. A-grade. |

### `defs_test.go`

| Test func | Validates | Notes |
|---|---|---|
| TestStringToNetID | NO-SPEC | NetID parser; no R covers parser semantics. |
| TestStringToNetIDErrors | NO-SPEC | Same, error paths. Useful regression but no spec anchor. |
| TestDowngradeTransMode | R-NOT-011 | TransMode auto-downgrade table. B-grade. |
| TestTransModeString | NO-SPEC | Stringer smoke test. |
| TestSymbolFlagContextMask | R-SYM-005 | ContextMask isolation from Flags. A-grade. |
| TestSymbolFlagHas | NO-SPEC | Flag helpers; supports R-SYM-005 indirectly. |
| TestSymbolFlagBitValue_Detection | NO-SPEC | Flag helper for BitValue; supports symbol-codec parsing. |
| TestReturnCodeString | NO-SPEC | Stringer mapping; no R covers ReturnCode strings. |
| TestReturnCodeError | NO-SPEC | error interface implementation; tautological. |
| TestReturnCodeString_AllCategories | NO-SPEC | Coverage-fill across error categories. C-grade. |
| TestReturnCodeError_ImplementsError | NO-SPEC | **TAUTOLOGY**: asserts non-empty error string for codes already covered by TestReturnCodeString. See Section 2. |
| TestBuildTag | R-ROUTE-001 (partial) | Tag encoding building block. B-grade. |
| TestAppendNull | NO-SPEC | Trivial helper; supports R-ROUTE-001. |
| TestProcessImageConstants | NO-SPEC | **COVERAGE THEATER**: asserts constants equal known hex values (uint32(GroupIoImageRwib) == 0xF020, etc.). Tests the constant declarations. See Section 2. |
| TestProcessImageBitOffset | NO-SPEC | **TAUTOLOGY**: asserts `byteOffset*8 + bitIndex == byteOffset*8 + bitIndex`. Pure arithmetic, no production function called. See Section 2. |
| TestADSTypeToString | R-SYM-004 (partial) | Maps ADST_ codes to type names. B-grade. |

### `cmd_notification_test.go` — handled above.

### `route_test.go`

| Test func | Validates | Notes |
|---|---|---|
| TestParseRouteResponse_RejectsInvokeIdMismatch | R-CMD-008 | Maps to T-U-701. A-grade. |
| TestParseRouteResponse_AcceptsMatchingInvokeId | R-CMD-008 | Happy-path complement. |
| TestBuildRoutePacket_EncodesInvokeId | R-CMD-008 | Build-side counterpart. |
| TestBuildRoutePacket | R-ROUTE-001 | Maps to T-U-900. Asserts every packet field. A-grade. |
| TestParseRouteResponse_Success | R-ROUTE-001 (parser side) | Happy-path parse; B-grade. |
| TestParseRouteResponse_ErrorCode | R-ROUTE-001 | Non-zero error tag rejected. |
| TestParseRouteResponse_TooShort | R-ROUTE-001 | Defensive length check. |
| TestParseRouteResponse_WrongCookie | R-ROUTE-001 | Cookie validation. |
| TestParseRouteResponse_WrongServiceID | R-ROUTE-001 | ServiceID validation. |

### `secret_test.go`

| Test func | Validates | Notes |
|---|---|---|
| TestSecret_StringRedacted | R-LOCK-004 | Maps to T-U-1101 partial (fmt path). A-grade. |
| TestSecret_LogValueRedacted | R-LOCK-004 | Maps to T-U-1101 partial (slog path). A-grade. |

### `session_test.go`

| Test func | Validates | Notes |
|---|---|---|
| TestZeroOldSymbolHandles | R-CACHE-004 | Maps to T-U-203 (partial). Only checks Handle field; spec requires Value, Valid, ValueParsed, LastUpdateTime cleared too. Partial coverage. |
| TestZeroOldSymbolHandles_NilSafe | R-CACHE-004 | Defensive nil/empty input. B-grade. |

### `symbol_codec_test.go`

| Test func | Validates | Notes |
|---|---|---|
| TestParse_RejectsOversizedSymbolLength | R-PARSE-006 | Maps to T-U-1007. A-grade. |
| TestParse_HappyPath_INT | R-PARSE-007 | Subset of T-U-1008 series. |
| TestWriteToNode_STRING_RejectsZeroLength | NO-SPEC strict | Defensive write-side check. Good test, no exact R but supports R-PARSE-003. |
| TestWriteToNode_WSTRING_RejectsTooShort | NO-SPEC strict | Same family for WSTRING (R-PARSE-004 area). |
| TestWriteToNode_FallsBackToInferredType | NO-SPEC | Asymmetric read/write convergence; F-18 history. No R covers the inferBaseType fallback for write. |
| TestWriteToNode_RejectsUnknownTypeWithUninferableSize | NO-SPEC | Complement to above. |
| TestWriteToNode_STRING_HappyPath | R-PARSE-003 | NUL-termination + truncation. Maps to T-U-1004 partial. |
| TestSymbolParseBasicTypes | R-PARSE-007 | Table-driven subset of T-U-1008..T-U-1027 (BOOL, INT, UINT, SINT, BYTE, DINT, LINT, ULINT, REAL, STRING). A-grade. |
| TestSymbolParseLREAL | R-PARSE-007 | LREAL coverage. |
| TestSymbolParseUnknownType | NO-SPEC | Error path for unknown size-3 type. |
| TestWriteToNodeRoundTrip | R-PARSE-007 (write/read symmetry) | Major round-trip table — 25+ types. A-grade. |
| TestWriteToNodeRoundTripFloat | R-PARSE-007 | REAL/LREAL round-trip with tolerance. |
| TestWriteToNodeStruct | R-PARSE-001 | Struct write with offsets, padding. A-grade. |
| TestWriteToNodeStructPartialFields | R-PARSE-001 | Partial JSON → unset fields zero. |
| TestWriteToNodeAliasResolution | NO-SPEC | Alias via datatypes map. |
| TestWriteToNodeAliasWithoutDatatypes | NO-SPEC | Error path with size 3. |
| TestWriteToNodeUnknownType | NO-SPEC | Same error family. |
| TestSymbolParseDataTooShort | R-PARSE-006 | Buffer-too-short table across types. A-grade. |
| TestSymbolParseSizeWrong | R-SYM-003 (partial) | BOOL with Length=2 rejected. |
| TestSymbolParseAliasResolution | NO-SPEC | Same alias topic. |
| TestWriteToNodeInvalidValues | NO-SPEC | Write rejects bad string repr. Useful negative coverage. |
| TestWriteToNodeStructInvalidJSON | NO-SPEC | Invalid JSON rejected. |
| TestSymbolParseSTRING_NoNullTerminator | R-PARSE-005 | Maps to T-U-1006. |
| TestSymbolParseSTRING_Empty | R-PARSE-005 | Edge case: leading NUL. |
| TestSymbolParseSTRING_TrailingGarbage | R-PARSE-005 | NUL stops parse. |
| TestWriteToNodeSTRING_PadsWithZeros | R-PARSE-003 | Buffer zero-fill. |
| TestWriteToNodeSTRING_ExactLength | R-PARSE-003 | NUL terminator at last byte. |
| TestWriteToNodeSTRING_Overflow | R-PARSE-003 | Truncate to Length-1. |
| TestSymbolParseSTRING_SpecialChars | R-PARSE-005 | Special chars before NUL. |
| TestSymbolParseFloatSpecial | R-PARSE-007 | NaN, +/-Inf, -0, subnormal. A-grade for IEEE-754 edges. |
| TestWriteToNodeFloatSpecial | R-PARSE-007 | Write side of NaN/Inf. |
| TestSymbolParseTemporalTypes | R-PARSE-007 | TIME/TOD/DATE/DT boundaries (epoch, leap, max-uint32, Y2K38). A-grade. |
| TestWriteToNodeTemporalRoundTrip | R-PARSE-007 | Date/time round-trip. |
| TestWriteToNodeTemporalAliases | R-PARSE-007 | TIME_OF_DAY, DATE_AND_TIME aliases. |
| TestParseNestedStructThreeLevels | R-PARSE-001 | 3-level nested struct parse. A-grade. |
| TestWriteToNodeNestedStruct | R-PARSE-001 | Nested struct write. |
| TestParseWSTRING_ASCII | R-PARSE-007 (WSTRING) | UTF-16LE ASCII. |
| TestParseWSTRING_Unicode | R-PARSE-007 (WSTRING) | Japanese chars. |
| TestParseWSTRING_NullTerminated | R-PARSE-007 (WSTRING) | Stop at UTF-16 NUL. |
| TestParseWSTRING_NoNullTerminator | R-PARSE-007 (WSTRING) | Buffer-full case. |
| TestParseWSTRING_Empty | R-PARSE-007 (WSTRING) | Leading NUL. |
| TestParseWSTRING_SurrogatePair | R-PARSE-007 (WSTRING) | U+1F600 emoji. |
| TestWriteWSTRING_ASCII | R-PARSE-004 / R-PARSE-007 | UTF-16LE write. |
| TestWriteWSTRING_Unicode | R-PARSE-004 | Multi-byte Unicode. |
| TestWriteWSTRING_Truncation | R-PARSE-004 | Truncation at maxChars. |
| TestWriteWSTRING_RoundTrip | R-PARSE-004 | Round-trip set. |
| TestParseBitSymbol_True | R-PARSE-007 | BOOL with BitValue flag set. |
| TestParseBitSymbol_False | R-PARSE-007 | Same, false case. |
| TestWriteBitSymbol_True | R-PARSE-007 | BOOL+BitValue write. |
| TestWriteBitSymbol_False | R-PARSE-007 | Same. |
| TestBitValueFlag_DoesNotOverrideUDINT | R-PARSE-007 | BitValue flag MUST NOT override UDINT parse. A-grade regression. |
| TestBitValueFlag_DoesNotOverrideLREAL | R-PARSE-007 | Same for LREAL. A-grade. |
| TestReadBit_Extract | NO-SPEC | Bit-extract helper. Useful for process-image work. |
| TestReadBit_AllPositions | NO-SPEC | All 8 bit positions. |
| TestWriteBit_Set | NO-SPEC | Bit-set helper. |
| TestWriteBit_Clear | NO-SPEC | Bit-clear helper. |
| TestWriteBit_PreservesOthers | NO-SPEC | Adjacent-bit preservation. |
| TestParseWithBaseType_REAL | R-SYM-004 | BaseType=ADST_REAL32 driving parse. A-grade. |
| TestParseWithBaseType_UDINT | R-SYM-004 | BaseType=ADST_UINT32 (unsigned vs signed). A-grade. |
| TestParseWithBaseType_FallbackToInfer | R-SYM-004 | BaseType=VOID falls through to inferBaseType. |

### `symbols_test.go`

| Test func | Validates | Notes |
|---|---|---|
| TestMakeArrayChildren_CapsExcessiveElements | R-SYM-002 (defense-in-depth) | 10M-element cap. A-grade defensive. |
| TestMakeArrayChildren_RejectsOverflowBound | R-SYM-002 | uint32 LBound+Elements overflow. A-grade. |
| TestMakeArrayChildren_HappyPath | NO-SPEC strict | 4-element array. B-grade. |
| TestParseUploadSymbolInfoDataTypes_Empty | NO-SPEC | Empty buffer → empty map. C-grade. |
| TestParseUploadSymbolInfoSymbols_Empty | NO-SPEC | Same. C-grade. |
| TestAddOffsetEmptySegmentName | NO-SPEC | F-06 historic fix; should be backfilled to spec. |
| TestAddOffsetFullNameWithDot | NO-SPEC | F-05 historic fix; full-name composition. |
| TestAddOffsetArrayFullName | NO-SPEC | F-05 array element naming. |
| TestParseEnumNestedInStruct | NO-SPEC | Enum sub-items must NOT expand as struct children. Two scenarios (non-strict + strict). A-grade regression but unanchored. |
| TestParseEnumWithoutDatatypes | NO-SPEC | On-demand mode enum size inference. A-grade regression but unanchored. |
| TestArrayTypedefNotMistakenForEnum | NO-SPEC | ArrayDim disambiguates from enum. A-grade regression but unanchored. |
| TestAddChildren | NO-SPEC | addChildren utility. |
| TestAddChildrenNoDuplicates | NO-SPEC | Doesn't overwrite existing entries. |
| TestMakeArrayChildren | NO-SPEC | 1-D 3-element array. C-grade. |
| TestMakeArrayChildrenEmpty | NO-SPEC | nil levels. |
| TestMakeArrayChildrenNonZeroLBound | NO-SPEC | LBound=5 yields keys [5], [6]. |
| TestMakeArrayChildren_2D | NO-SPEC | 2-D array hierarchical children. |
| TestMakeArrayChildren_3D | NO-SPEC | 3-D array nesting. |
| TestMakeArrayChildren_ZeroElements | NO-SPEC | Zero-length array. |
| TestInferBaseType | NO-SPEC | Size→type mapping. Helper for parse. |
| TestGetJSON | NO-SPEC | Sym.GetJSON helper. |
| TestGetJSONBool | NO-SPEC | Bool JSON encoding. |
| TestGetJSONString | NO-SPEC | String JSON quoting. |
| TestGetJSONStruct | NO-SPEC | Nested-children JSON. |
| TestGetJSON_EmptyValue | NO-SPEC | Empty STRING → "". |
| TestGetJSON_NumericOverflow | NO-SPEC | **COVERAGE THEATER**: asserts only `j != ""` for ULINT max. See Section 2. |
| TestGetJSON_WSTRINGAsString | NO-SPEC | WSTRING JSON quoting. |
| TestParseUploadSymbolInfoSymbols_SingleSymbol | R-CACHE-006 (case-insensitive key) | Verifies lowercased map key + preserved FullName casing. A-grade. |
| TestParseUploadSymbolInfoSymbols_TruncatedEntry | R-CMD-007 | Truncated upload data rejected. |
| TestSymbolSumAddress_PrefersHandleOverDirect | NO-SPEC | symbolSumAddress preference logic. Should anchor in a new R-SUM (handle preference). |
| TestSymbolSumAddress_HandleOnlyNoGroup | NO-SPEC | Handle-only addressing. |
| TestSymbolSumAddress_DirectFallbackNoHandle | NO-SPEC | Direct fallback path. |
| TestSymbolSumAddress_DirectFallbackChildAccumulatesOffset | NO-SPEC | Child offset accumulation up parent chain. |
| TestSymbolSumAddress_DirectFallbackNestedChild | NO-SPEC | 3-level nesting accumulation. |

### `review_round4_test.go`

| Test func | Validates | Notes |
|---|---|---|
| TestAddOffsetDepthCap | R-SYM-002 | Maps to T-U-300. A-grade. |
| TestCollectSubtreeDepthCap | R-VIEW-004 | Maps to T-U-404. A-grade. |
| TestWSTRINGSurrogatePairTruncation | R-PARSE-004 | Maps to T-U-1005. A-grade — high-surrogate dropped at truncation boundary. |
| TestSumNotificationResultTriState | R-NOT-009 / R-SUM-004 | Maps to T-U-110. **MIMIC RISK**: re-implements the tri-state predicate (`Skipped != nil`, etc.) in the test; same logic the docs describe. See Section 2. |

### `browse_test.go`

| Test func | Validates | Notes |
|---|---|---|
| TestBrowseAllSymbols | R-CACHE-001 / R-VIEW-005 | Integration-tagged. Dumps a `.var` file from PLC; asserts only loaded count > 0. C-grade as a behavioral check; useful as a one-shot diagnostic. |

### `testconn_integration_test.go`

Helpers only — no test functions.

---

## Section 2 — Coverage theater + tests that mimic implementation

Each entry: file:line, test name, category, diagnosis, action.

### A1 — `cmd_notification_test.go:358` `TestWindowsFiletimeConversion`
**Category: A (Mimics implementation).** The production handler computes `ts = int64(filetime)/windowsTick - secToUnixEpoch`. The test computes the encoding `filetime = (unixSec + secToUnixEpoch) * windowsTick`, then immediately reverses it with the *same constants* — `int64(filetime)/windowsTick - secToUnixEpoch` — and checks that the year/month/day match. The production code is never called. If the formula were wrong by a constant, both sides would shift in lockstep and the test would still pass. The companion `TestDeviceNotification_SingleSample` exercises the real code path with a non-trivial unix timestamp; that one is sufficient.
**Action: REWRITE.** Keep one or two table cases that feed a hard-coded raw FILETIME (e.g. `filetime = 132223104000000000` for 2020-01-01 UTC) directly into a `Symbol.Notification` channel via the production deviceNotification path and assert the user's `update.TimeStamp` equals the expected `time.Time`. Remove the back-and-forth re-derivation.

### A2 — `cmd_sum_test.go:140` `TestSumReadOverflowGuard`
**Category: C (Tautological).** The body is:
```go
var totalReadLen uint64
for _, req := range requests { totalReadLen += uint64(req.Length) }
total := uint64(n)*8 + totalReadLen
if total <= math.MaxUint32 { t.Fatalf(...) }
```
No production function is invoked. The test asserts that adding two known constants overflows uint32. It would pass even if the production overflow guard were deleted.
**Action: REWRITE.** Drive `Client.SumRead` (or `executeSumCommand`) with a request slice whose summed Length crosses `MaxUint32`, capture either the rejection error or the encoded WriteRead length field, and assert the production code refused to under-allocate. If that integration is too heavy at unit level, delete this test.

### A3 — `defs_test.go:298` `TestProcessImageConstants`
**Category: B (Coverage theater) / D (untraced behavior).** Asserts `uint32(GroupIoImageRwib) == 0xF020` and similar for 8 constants. These constants are defined as those exact literals in `defs.go`. Any change to one would simultaneously break the test by definition — but a wrong constant in `defs.go` (e.g. typo making it 0xF021) would silently match a wrong test value if both were edited together. There is no R-XX requirement saying these constants MUST equal those values; PROTOCOL.md is the source of truth and is not consulted by the test.
**Action: KEEP-WITH-REASON or REWRITE.** Either (a) cite `PROTOCOL.md§Process Image Groups` in a comment and treat the test as a regression guard, or (b) replace with an integration test that issues `Read(GroupIoImageRwib, 0, 1)` against a TC3 PLC and observes the PLC accepts the group ID. Currently it is a snapshot of the source file.

### A4 — `defs_test.go:322` `TestProcessImageBitOffset`
**Category: C (Tautological).** Body: `got := tt.byteOffset*8 + uint32(tt.bitIndex); if got != tt.want { ... }`. The expected value is computed by hand on the same formula. No production function is called. This tests Go's multiplication operator.
**Action: DELETE.** No production code is exercised. If process-image bit addressing has a real helper, rewrite to call that helper.

### A5 — `defs_test.go:249` `TestReturnCodeError_ImplementsError`
**Category: C (Tautological).** Asserts `err.Error() != ""` for codes already covered by `TestReturnCodeString_AllCategories` (which checks the actual content). The remaining content of this test is a smoke-loop that adds nothing.
**Action: DELETE.** Redundant.

### A6 — `symbols_test.go:581` `TestGetJSON_NumericOverflow`
**Category: B (Coverage theater).** Asserts only `if j == "" { t.Error(...) }` — i.e. "the function returned a non-empty string". The test name implies it should validate precision behavior for ULINT max; it does not. The comment even acknowledges "parseFloat loses precision for uint64 max — this is a known limitation". The test cannot fail unless GetJSON returns "" for ULINT max, which it doesn't.
**Action: REWRITE.** Either assert the documented lossy float-string output literally (e.g. `"1.8446744073709552e+19"`) and add a comment that this limitation is intentional, or replace with a test that ULINT max is encoded as a JSON *string* (lossless) — which would require a code change. As written, the test is filler.

### A7 — `review_round4_test.go:121` `TestSumNotificationResultTriState`
**Category: A (Mimics implementation).** The test computes `isSkipped := tc.result.Skipped != nil`, `isPLCErr := tc.result.Skipped == nil && tc.result.Error != ReturnCodeNoErrors`, `isSuccess := tc.result.Skipped == nil && tc.result.Error == ReturnCodeNoErrors` — the same predicate the documentation prescribes — and then asserts those derived booleans match the table. The production library never sees this struct in the test; the test re-derives the documented contract from the field values. If the library's internal classification logic diverged from the doc, this test would still pass because it only tests the docstring's three rules against the docstring's three input shapes.
**Action: REWRITE.** Drive `AddSymbolNotifications` or `SumAddDeviceNotification` with a synthetic Client that returns:
1. a duplicate-name request → expect `Skipped != nil, Handle == 0`;
2. a PLC-error response (`Error = ReturnCodeDeviceError`) → expect `Skipped == nil, Error != NoErrors`;
3. a successful response → expect `Skipped == nil, Error == NoErrors, Handle != 0`;
4. (the killer case) a TOCTOU race that produces `Skipped != nil, Handle != 0` so the caller knows it must call DeleteDeviceNotification.
That exercises the actual classification code, not the docstring.

### A8 — `client_test.go:108` `TestClient_DialFailsWhenServerUnreachable`
**Category: A (Mimics implementation, weak).** Asserts `strings.Contains(err.Error(), "ads: dial 127.0.0.1:1")` — couples the test to the exact error-string format. If `cmd_simple.go`/`client.go` ever changes the wrap format, this test breaks even though behavior is correct. Comment-of-intent says this is a smoke test for "Dial returns wrapped error" which would be better expressed by `errors.Is` with a typed sentinel.
**Action: KEEP-WITH-REASON** for now; flag in Section 4 to swap to `errors.Is(err, net.OpError)` or a typed `ErrDialFailed` sentinel once the library introduces one.

### A9 — `client_test.go:454` `TestEncodePacket_AllCommands`
**Category: B (Coverage theater).** Loops 8 CommandIDs and only asserts `binary.LittleEndian.Uint16(packet[22:24]) == cmd`. Does not check InvokeID was distinct, payload was correct, or AMS state was 4. A bug encoding the *payload* with the wrong command would not be caught. The test reads as a coverage line-hit walker.
**Action: REWRITE or DELETE.** `TestEncodePacket` already exercises one CommandID end-to-end. Either remove this loop (the per-command logic is identical — there's no per-command code path) or extend it to assert payload + state + invokeID round-trip.

### A10 — `defs_test.go:180` `TestReturnCodeString_AllCategories`
**Category: B (Coverage theater).** 35 codes in a table; each asserts `strings.Contains(...)` against a hand-typed substring of that code's known message. If the message text is tweaked (e.g. "timeout elapsed" → "request timed out"), this test breaks even though the `String()` implementation is correct. The test acts as a snapshot of message strings without a corresponding requirement that says the messages MUST have specific text.
**Action: KEEP-WITH-REASON.** Keep as a snapshot so message changes are deliberate, but document in `02-quality-constitution.md` exception or cite a future R-DOC for ReturnCode message stability. Alternatively cut it back to 5 representative codes.

### B1 — Tests with `NO-SPEC` Validates that should produce a spec backfill rather than deletion (these are real regression guards):

- `symbols_test.go:78,92,109` Add-offset tests — anchor in a new R-SYM (full-name composition rules + array-element naming).
- `symbols_test.go:129,254,307` Enum / array-typedef classification — anchor in a new R-SYM (enum constants must not expand as struct children).
- `symbols_test.go:684..760` symbolSumAddress preference — anchor in a new R-SUM (handle-based addressing preference + offset accumulation).
- `client_test.go:302,324` `isLikelyMissingRoute` heuristic — anchor in R-ROUTE-004 extension (route-probe error classification).
- `defs_test.go:11,36` stringToNetID — anchor in R-SES-001 sub-clause (NetID parsing rules).

These are NOT theater; they are unanchored real tests. Treat as **spec backfill candidates** in Section 4.

---

## Section 3 — Missing test coverage

> **Resolution status (2026-05-09 update):** the items marked
> **RESOLVED** below were filled by the Tier 3 backfill commits
> (`d180f54`) and the scriptable-PLC-stub follow-on (`34c4de3`).
> Specifically:
>
> - **R-CL-001 / R-CL-002 / R-CL-003 / R-CL-004 / R-CL-006 / R-CL-008** — RESOLVED
> - **R-SES-001 / R-SES-005 / R-SES-006 / R-SES-008** — RESOLVED
> - **R-CACHE-002 / R-CACHE-003 / R-CACHE-007 / R-CACHE-008** — RESOLVED
> - **R-VIEW-001 / R-VIEW-002 / R-VIEW-003 / R-VIEW-004 / R-VIEW-005 / R-VIEW-007** — RESOLVED
> - **R-RECON-002 / R-RECON-008 / R-RECON-009** — RESOLVED
> - **R-NOT-001 / R-NOT-002 / R-NOT-003 / R-NOT-004 / R-NOT-008 / R-NOT-010 / R-NOT-013 / R-NOT-015** — RESOLVED (NOT-004 was the highest-priority Round-4 stranded-Symbol gap; pinned by `TestAddSymbolNotification_StrandedSymbol_DetectedByEpoch`)
>
> Items still missing as of 2026-05-09:
>
> - **R-SES-002 / R-SES-009** — integration-only coverage; no unit
>   complement. Acceptable.
> - **R-SES-007 / R-SES-010** — minor (callback goroutine isolation,
>   logger immutability). Low priority.
> - **R-SES-011** — DP-1 options. Blocked on task #33.
> - **R-NOT-005 / R-NOT-006 / R-NOT-007 / R-NOT-011 / R-NOT-014** —
>   partial coverage. The cases not exercised are integration-style
>   (e.g. R-NOT-014 reconnect rollback).
> - **R-NOT-016 / R-NOT-017** — DP-1. Blocked on task #33.
> - **R-CACHE-001 / R-CACHE-005 / R-CACHE-006 / R-CACHE-014..018** —
>   integration-only or DP-1. R-CACHE-014..018 specifically blocked on
>   DP-3 implementation.
> - **R-CACHE-009..013** — DP-1. Blocked on task #33.
> - **R-VIEW-006** — integration-only.
> - **R-TX-001 / R-TX-005 / R-TX-006 / R-TX-008** — Phase 5 transport
>   refactor partially obsoleted these (R-TX-005 wait-for-reconnect
>   moved to Session.waitForReconnect, covered by Tier 3 retry helpers).
> - **R-RECON-003 / R-RECON-010** — minor; Phase 4 partially
>   subsumed via epoch unification.
> - **R-RECON-004 / R-RECON-005 / R-RECON-006 / R-RECON-007** —
>   integration-only.
> - **R-CMD-001..005** — integration-only.
> - **R-CMD-006** — wire-format unit test still missing.
> - **R-PARSE-002** — `parentChanged` was removed in DP-2; spec entry
>   should be deprecated.
> - **R-SYM-001 / R-SYM-003 / R-LOCK-003 / R-LOCK-005** — doc-content
>   tests; T-DOC-* IDs missing.
>
> Below: original pre-resolution gap analysis. Retained as historical
> reference — see "Resolution status" above for the post-Tier-3 state.


Requirements with no traceable test, grouped by where the test would land. Verification field of each requirement gives a hint about the test ID.

### Module SES — Session lifecycle

Tests should land in `session_test.go` (currently only has zeroOldSymbolHandles tests).

- **R-SES-001** — `NewSession` does no I/O, defaults applied. *Unit.* Construct, assert no goroutines started, requestTimeout default 5s, localPort default 10500.
- **R-SES-002 (Connect happy path)** — *Integration.* Currently TestIntegrationConnect, but no traceability comment. Backfill comment.
- **R-SES-003 (Close idempotency, listen+transmit+recvWorker exit)** — *Functional* via net.Pipe. Currently no test.
- **R-SES-004 (IsDisconnected reflects state)** — *Integration* exists conceptually; no unit functional test.
- **R-SES-005 (Connect after Close rejected)** — *Unit.* Construct, Close, attempt Connect, expect error.
- **R-SES-006 (option validation)** — *Unit.* `WithLogger(nil)` no-op, `WithRequestTimeout(0)` no-op, `WithRoute("")` no-op, `WithBackoff{}` falls back to default. None tested.
- **R-SES-007 (callbacks fire in own goroutine)** — *Unit.* Capture goroutine ID inside callback; assert different from listen goroutine. No test.
- **R-SES-008 (onDisconnect fires once)** — *Unit.* Synthetic concurrent triggerReconnect; CAS gate. No test.
- **R-SES-009 (onReconnect order)** — *Integration.*
- **R-SES-010 (logger non-nil, immutable)** — *Unit.* No test.
- **R-SES-011 (online-change options)** — *Unit + Integration.* Implementation pending (task #33). No test.

### Module CL — Client (Phase 5 added these; tests are partial)

Tests should land in `client_test.go`.

- **R-CL-001** — partially covered (TestClient_DialClose, TestClient_DoubleClose). Add traceability comments. Add test that all goroutines exited after Close (count via `runtime.NumGoroutine`).
- **R-CL-002 — concurrent multiplexing safety** — *Unit.* 100 concurrent `client.send` invocations under -race. No test exists; risk is high after Phase 5 split.
- **R-CL-003 — closed Client returns ErrTransportClosed from every method** — partial (TestClient_TransportClosedSentinel checks errors.Is only). Add: Close, then call `client.Read` / `client.Write` / `client.AddDeviceNotification` and assert each returns ErrTransportClosed.
- **R-CL-004 — goroutines bounded** — *Unit.* Dial against stub, count goroutines (`runtime.NumGoroutine` baseline + delta), Close, assert delta == 0.
- **R-CL-005 — no live state after Close** — *Unit.* Close, attempt to use `client.tx` field, assert ErrTransportClosed.
- **R-CL-006 — ReleaseHandle wraps Write** — *Integration.* No test in either unit or integration suite.
- **R-CL-007 — notification dispatch** — partial (TestClient_OptionsApplied installs the handler; TestDeviceNotification_SingleSample fires through Session). Add a test with **only** a Client (no Session) that installs a handler via SetNotificationHandler and verifies a synthetic packet drives it.
- **R-CL-008 — on-drop callback fires once** — *Unit.* Install SetOnDrop, simulate listen failure (e.g. close net.Pipe), assert callback fires exactly once. Currently no test for this — the production code in `client.go:237` `callOnDrop` is only smoke-tested through TestListen_OversizePacketTriggersReconnect (which doesn't install a callback).

### Module NOT — Notifications

Tests should land in a new `notifications_test.go` (currently `cmd_notification_test.go` covers parsing only, `notification_api.go` is the production file).

- **R-NOT-001** — channel-mismatch rejected. Spec ID T-U-100. **Missing.**
- **R-NOT-002** — duplicate-symbol rejected (single, in-batch, cross-batch). T-U-101..103. **Missing.**
- **R-NOT-003** — TOCTOU re-check after roundtrip. T-U-104. **Missing.**
- **R-NOT-004** — generation bump mid-roundtrip. T-U-105. **Missing.** (CRITICAL.)
- **R-NOT-005** — handleNotification re-resolves via FullName. T-U-106. *Partially* covered by TestDeviceNotification_SingleSample but not the stranded-pointer scenario.
- **R-NOT-006** — non-blocking dispatch on slow consumer. T-U-107. Partially covered (TestDeliverNotification_DropsWhenChannelFull uses a buffered channel with one filler; need a test where the listen goroutine is observably not blocked).
- **R-NOT-007** — first-sample race window. T-U-108. *Partially* covered by TestDeviceNotification_UnknownHandleDuringClose; need the lastSubscribeNs path explicitly.
- **R-NOT-008** — DeleteDeviceNotification clears state, treats 0x745 as success. T-U-109. **Missing.**
- **R-NOT-010** — channel set only on first success. T-U-111. **Missing.**
- **R-NOT-011** — auto-downgrade in-context modes. T-U-112. *Partially* covered (TestDowngradeTransMode tests the helper) but not the AddSymbolNotification end-to-end downgrade with a Warn log.
- **R-NOT-013** — resubscribe retry up to max. T-U-115/T-U-116. **Missing.**
- **R-NOT-014** — resubscribe rolls back on transport error. **Integration only**, T-I-009. Currently in integration_test.go as TestIntegrationReconnectDuringBatchRead which is a different scenario. Check.
- **R-NOT-015** — bestEffortDelete logs error propagation. T-U-118. *Partially* covered (Empty case tested, mixed-success case missing).
- **R-NOT-016** — Update.Stale + Update.Reason fields (DP-1). T-U-119. **Missing entirely** (DP-1 implementation pending).
- **R-NOT-017** — dispatch under stale-cache strategies. T-I-046. **Missing entirely** (DP-1 implementation pending).

### Module CACHE — Symbol cache

Tests should land in a new `cache_test.go` (currently no dedicated file).

- **R-CACHE-001** — opt-in discovery. *Integration* covered indirectly. No unit test.
- **R-CACHE-002** — cache.lock guards mutations. T-U-200. **Missing** (race-detector test).
- **R-CACHE-003** — generation bumps on swap, NOT on insert. T-U-201/T-U-202. **Missing.** (HIGH priority — core invariant.)
- **R-CACHE-004** — zeroOldSymbolHandles clears all fields. T-U-203. *Partially* covered (only Handle field tested in session_test.go). Need a test that Value, Valid, ValueParsed, LastUpdateTime are all cleared.
- **R-CACHE-005** — onDemandSymbols tracks caller requests. T-U-204. **Missing.**
- **R-CACHE-006** — symbolKey case-insensitive. T-U-205. *Partially* covered by TestParseUploadSymbolInfoSymbols_SingleSymbol; need a test that mixed-case getSymbol calls hit the same cache entry.
- **R-CACHE-007** — getSymbol on-demand resolution + concurrent duplicate-handle release. T-I-015. **Missing.** (HIGH priority.)
- **R-CACHE-008** — never hold cache.lock + notifications.lock simultaneously. T-U-206/T-U-207. **Missing.** (CRITICAL.)
- **R-CACHE-009..013** — DP-1 stale-cache detection + 3 strategies + reload cap. T-U-220/T-U-221/T-U-222 + T-I-040..T-I-044. **Missing entirely** (DP-1 implementation pending).
- **R-CACHE-014** — cache state machine. T-U-230. **Missing entirely** (DP-3).
- **R-CACHE-015** — discovery mode orthogonality. T-U-231. **Missing.**
- **R-CACHE-016** — mixed eager + on-demand. T-U-232. **Missing.** (Integration T-I-050 exists; unit complement needed.)
- **R-CACHE-017** — RefreshSymbols semantics. T-U-233. **Missing.**
- **R-CACHE-018** — generation-based view staleness. T-U-234. **Missing.**

### Module SYM — Symbol type

Tests in `symbols_test.go` (mostly NO-SPEC currently).

- **R-SYM-001** — field-guard godoc. T-DOC-001. **Missing.** (Doc-content test.)
- **R-SYM-002** — Children/Parent acyclic + depth caps. T-U-300. ✅ Covered (TestAddOffsetDepthCap, TestCollectSubtreeDepthCap).
- **R-SYM-003** — Symbol.Length wire-data size. T-I-016. *Partially* covered indirectly by TestSymbolParseSizeWrong; need explicit STRING(N) and WSTRING(N) length-includes-NUL test (integration ideal).
- **R-SYM-004** — BaseType ADST_ code. T-I-017. ✅ Covered by TestParseWithBaseType_REAL/UDINT/FallbackToInfer.
- **R-SYM-005** — ContextMask isolation. T-U-301. ✅ Covered by TestSymbolFlagContextMask.
- **R-SYM-006** — STRING/WSTRING type-name normalization. T-U-302. **Missing** (the function `normalizeStringDataType` exists in `symbols.go:61` but has no direct unit test).

### Module VIEW — SymbolView

Tests should land in a new `symbol_view_test.go`.

- **R-VIEW-001** — snapshot consistency. T-U-400. **Missing.** (HIGH — race-detector test.)
- **R-VIEW-002** — IsValid distinguishes zero from live. T-U-401. **Missing.**
- **R-VIEW-003** — Children returns fresh map. T-U-402. **Missing.**
- **R-VIEW-004** — ChildrenWalk collect-then-iterate (no deadlock). T-U-403. **Missing.** (Cycle cap covered by TestCollectSubtreeDepthCap.)
- **R-VIEW-005** — ListSymbols requires full discovery. T-U-405. **Missing.**
- **R-VIEW-006** — GetSymbol on-demand resolves. T-I-019. *Integration* covers; no unit equivalent.
- **R-VIEW-007** — read-after-Close is safe (snapshot semantics). T-U-406. **Missing.**

### Module TX — Transport

Tests in `client_test.go` partially.

- **R-TX-001** — single TCP per Connection (no socket leak across reconnects). T-U-500. **Missing.**
- **R-TX-002** — bufio framing on split packets. ✅ Covered (TestListen_TwoSequentialPackets).
- **R-TX-003** — 4-MiB sanity cap. ✅ Covered (TestListen_OversizePacketTriggersReconnect).
- **R-TX-004** — per-invoke ID multiplexing. ✅ Partial (TestHandleReceive_RoutesToCorrectChannel + TestHandleReceive_UnknownInvokeID). Missing: concurrent-correlation test (T-U-503).
- **R-TX-005** — sendRequest retries on ctx-canceled during reconnect, not on deadline. T-U-505/T-U-506. **Missing.** (Note: spec test uses `Connection.sendRequest`; Phase 5 split moved this to `Client.sendRequest`. Re-anchor in spec.)
- **R-TX-006** — bounded recvWorker pool, queue overflow drops. T-U-507. **Missing.**
- **R-TX-007** — System packets use shared systemResponse channel. T-U-508. *Implicitly* covered by TestListen_TwoSequentialPackets.
- **R-TX-008** — chanMu guards channel reassignment. T-U-509. **Missing.** (HIGH — race-detector test.)

### Module RECON — Reconnect FSM

Tests should land in a new `reconnect_test.go`.

- **R-RECON-001** — auto vs manual reconnect. *Integration* T-I-021/T-I-022 covers.
- **R-RECON-002** — single-flight triggerReconnect. T-U-600. **Missing.** (CRITICAL.)
- **R-RECON-003** — backoff stepped + capped. T-U-601. **Missing.**
- **R-RECON-004** — maxReconnectAttempts limit. T-I-023. *Integration only.*
- **R-RECON-005** — epoch counter (folded from reconnectGeneration). T-I-024. *Integration only.*
- **R-RECON-006** — Reconnect re-establishes route + symbols + notifications. T-I-025. *Integration only.*
- **R-RECON-007** — strict reconnect mode. T-I-026. *Integration only.*
- **R-RECON-008** — WaitGroup add/wait ordering on Close-during-dial. T-U-602/T-U-603. **Missing.** (CRITICAL — race-detector test for the F-02 fix.)
- **R-RECON-009** — reconnect goroutine exits via reconnectDone. T-U-604. **Missing.**
- **R-RECON-010** — disconnected flag flips false in dialAndStart. T-U-605. **Missing.**

### Module CMD — ADS commands

- **R-CMD-001** — ReadDeviceInfo. T-I-027. *Integration only.*
- **R-CMD-002** — Read. T-I-028. *Integration only.*
- **R-CMD-003** — Write. T-I-029. *Integration only.*
- **R-CMD-004** — ReadState. T-I-030. *Integration only.*
- **R-CMD-005** — WriteRead. T-I-031. *Integration only.*
- **R-CMD-006** — AddDeviceNotification wire format (40 bytes). T-I-032. **Missing as unit.** Should have a synthetic packet builder + decoder that round-trips the request body.
- **R-CMD-007** — body-length validation. T-U-700. *Partially* covered (TestHandleReceive_TooShort, TestParseSumReadResponse_Truncation, TestDeviceNotification_EmptyPacket).
- **R-CMD-008** — invokeID per-request random for route. T-U-701. ✅ Covered (TestParseRouteResponse_RejectsInvokeIdMismatch + TestBuildRoutePacket_EncodesInvokeId).

### Module SUM

- **R-SUM-001** — SumRead three-tier fallback. T-I-033..035. *Integration covers.*
- **R-SUM-002** — SumWrite single fallback. T-I-036. *Integration.*
- **R-SUM-003** — independent CAS for sumAdd/sumDelete. T-U-800. ✅ Covered (TestSumProbeStateTransitions).
- **R-SUM-004** — SumNotificationResult tri-state. T-U-110. Currently TestSumNotificationResultTriState is **mimic-style** (Section 2 A7); rewrite required.
- **R-SUM-005** — 500-item soft limit doc. T-DOC-002. **Missing** (godoc-content test).
- **R-SUM-006** — parseSumReadResponse data-section integrity. T-U-801. ✅ Covered (TestParseSumReadResponse_LengthOverflow / _Truncation / _PerItemOversize).
- **R-SUM-007** — executeSumCommand orchestration. **Missing** as a unit test (covered transitively by integration).

### Module ROUTE

- **R-ROUTE-001** — UDP 48899 packet build. T-U-900. ✅ Covered.
- **R-ROUTE-002** — see R-CMD-008.
- **R-ROUTE-003** — credentials in plaintext, doc warning. T-DOC-003. **Missing** (godoc-content test).
- **R-ROUTE-004** — probe before register. T-I-040. *Integration.*
- **R-ROUTE-005** — WithForceRouteRegistration. T-I-041. *Integration.*

### Module PARSE

- **R-PARSE-001** — parse walks Symbol tree. T-U-1000/T-U-1001. *Partially* covered (TestSymbolParseBasicTypes, TestParseNestedStructThreeLevels).
- **R-PARSE-002** — parentChanged walks ancestors. T-U-1002/T-U-1003. **Missing entirely.** (No test for the Changed propagation up the parent chain.)
- **R-PARSE-003** — STRING write null-termination. T-U-1004. ✅ Covered (TestWriteToNodeSTRING_*).
- **R-PARSE-004** — WSTRING surrogate-pair truncation. T-U-1005. ✅ Covered (TestWSTRINGSurrogatePairTruncation).
- **R-PARSE-005** — STRING parse stops at first NUL. T-U-1006. ✅ Covered.
- **R-PARSE-006** — symbol.Length oversize protection. T-U-1007. ✅ Covered (TestParse_RejectsOversizedSymbolLength + TestSymbolParseDataTooShort).
- **R-PARSE-007** — type-specific parse. T-U-1008..T-U-1027. ✅ Covered for ~20 types via TestSymbolParseBasicTypes/TemporalTypes/FloatSpecial/WSTRING/Bit. The spec wants one test per type — current setup uses table-driven sub-tests, which is acceptable.

### Module LOCK

- **R-LOCK-001** — see R-CACHE-008.
- **R-LOCK-002** — `go test -race` clean. CI invariant. ✅ Implicit.
- **R-LOCK-003** — atomic types boundary audit. T-U-1100. **Missing** (doc-content / static-analysis test).
- **R-LOCK-004** — secret redaction. T-U-1101. ✅ Covered (TestSecret_StringRedacted, TestSecret_LogValueRedacted).
- **R-LOCK-005** — per-aggregate lock granularity doc. T-DOC-004. **Missing** (godoc-content test).

### Aspirational integration tests still pending (recap from `05-integration-tests.md`)

- T-I-006 (TC3 online-change mid-notification) — DP-1 hardware test deferred.
- T-I-020 (high-rate notifications, 1000/s for 60s) — load test.
- T-I-038b (50k symbols) — large-table stress.

---

## Section 4 — Recommended action plan

> **Status as of 2026-05-09**: items 1–18 DONE.
>
> - Tier 1 (items 1, 2, 8, 18 — quick deletes + comment + strengthen)
>   landed in commit `5ad0190`.
> - Tier 2 (items 3, 4, 5, 6, 7 rewrites + items 9, 10, 11 spec
>   backfill) landed in commit `78f934c`. Item 6 deferred only
>   for the 30-min stub-server prerequisite, then completed in
>   commit `34c4de3` follow-on.
> - Tier 3 (items 12, 13, 14, 15, 16, 17 — fill missing-test gaps)
>   landed in commit `d180f54`. The 5 t.Skip TODOs in that commit
>   were unblocked by commit `34c4de3` (scriptable PLC stub server).
> - Item 19 (traceability comments on every test) — partial. Tier
>   1–3 tests already carry `// Validates: R-XXX-NNN` comments;
>   pre-Tier tests in `symbol_codec_test.go`, `symbols_test.go`,
>   `defs_test.go`, `browse_test.go`, `secret_test.go`,
>   `route_test.go` still don't. Low-priority cleanup.
> - Item 20 (DP-1 / DP-3 missing tests) — DEFERRED per task #33.
>
> Original action plan retained below as historical record.


Each item ≤15 minutes of engineering work. Order is roughly: delete cleanly first, then rewrite mimics, then backfill gaps and traceability.

1. **DELETE** `defs_test.go:322` `TestProcessImageBitOffset` — pure arithmetic, no production code.
2. **DELETE** `defs_test.go:249` `TestReturnCodeError_ImplementsError` — redundant with TestReturnCodeString_AllCategories.
3. **REWRITE** `cmd_notification_test.go:358` `TestWindowsFiletimeConversion` — drop the back-and-forth re-derivation; feed hard-coded raw FILETIME values into the production deviceNotification path and assert `Update.TimeStamp`.
4. **REWRITE** `cmd_sum_test.go:140` `TestSumReadOverflowGuard` — call the production overflow guard with a request slice whose summed Length exceeds MaxUint32; assert the rejection error. Otherwise delete.
5. **REWRITE** `symbols_test.go:581` `TestGetJSON_NumericOverflow` — assert the actual lossy float-string output for ULINT max, with a comment that the limitation is intentional. Or replace with a string-encoding requirement.
6. **REWRITE** `review_round4_test.go:121` `TestSumNotificationResultTriState` — replace docstring re-derivation with four end-to-end scenarios driving real `AddSymbolNotifications` paths (success, PLC error, library skip, TOCTOU race producing Skipped+Handle).
7. **REWRITE or KEEP-WITH-REASON** `client_test.go:454` `TestEncodePacket_AllCommands` — extend to assert payload+state+invokeID for each command, or delete (TestEncodePacket already covers the encode path).
8. **KEEP-WITH-REASON** `defs_test.go:298` `TestProcessImageConstants` — add a one-line comment citing PROTOCOL.md§Process Image Groups.
9. **Spec backfill — symbols**: add `R-SYM-007` (FullName composition rules incl. dotted parents and array-element brackets) to `01-requirements.md` and add `// Validates: R-SYM-007` comments to TestAddOffsetEmptySegmentName, TestAddOffsetFullNameWithDot, TestAddOffsetArrayFullName.
10. **Spec backfill — enum classification**: add `R-SYM-008` (enum constants must NOT be expanded as struct children; on-demand mode size-inference) and anchor TestParseEnumNestedInStruct, TestParseEnumWithoutDatatypes, TestArrayTypedefNotMistakenForEnum.
11. **Spec backfill — sum addressing**: add `R-SUM-008` (handle-based address preference + parent-offset accumulation in `symbolSumAddress`); anchor the five TestSymbolSumAddress_* tests.
12. **Add unit tests for R-CL-001..R-CL-008** in `client_test.go`:
   - R-CL-002: 100 concurrent `client.send` invocations, race-detector clean.
   - R-CL-003: post-Close, every public method returns ErrTransportClosed.
   - R-CL-004: count goroutines pre-Dial / post-Dial / post-Close.
   - R-CL-006: ReleaseHandle wraps Write to GroupSymbolReleaseHandle.
   - R-CL-008: SetOnDrop fires exactly once on synthetic listen failure.
13. **Add unit tests for R-SES-001..R-SES-008** in `session_test.go`:
   - R-SES-001: NewSession total construction (no I/O, defaults applied).
   - R-SES-005: Connect after Close rejected.
   - R-SES-006: each `WithXxx(zero)` is no-op.
   - R-SES-008: synthetic concurrent triggerReconnect → onDisconnect fires once.
14. **Add unit tests for R-CACHE-002, R-CACHE-003, R-CACHE-007, R-CACHE-008** in a new `cache_test.go`:
   - R-CACHE-003: assert `sessionFSM.epoch` bumps on `loadSymbols` / `LoadSymbolList` / `LoadDataTypes` (via `bumpEpoch()`), NOT on on-demand inserts.
   - R-CACHE-007: concurrent on-demand resolve, both goroutines see the same `*Symbol`, duplicate handle released.
   - R-CACHE-008: race-detector test driving concurrent acquirers of cache.lock and notifications.lock.
15. **Add unit tests for R-VIEW-001..R-VIEW-005, R-VIEW-007** in a new `symbol_view_test.go`:
   - R-VIEW-001: snapshot stays consistent under concurrent loadSymbols.
   - R-VIEW-002: zero-value vs live IsValid.
   - R-VIEW-005: ListSymbols error before LoadSymbols.
   - R-VIEW-007: SymbolView field-read after Close returns the snapshot.
16. **Add unit tests for R-RECON-002, R-RECON-003, R-RECON-008, R-RECON-009** in a new `reconnect_test.go`:
   - R-RECON-002: concurrent triggerReconnect → exactly one goroutine started.
   - R-RECON-008: Close during in-progress dial — race-detector clean, no waitGroup misuse panic.
17. **Add unit tests for R-NOT-001..R-NOT-004, R-NOT-008, R-NOT-010, R-NOT-013, R-NOT-015** in a new `notifications_test.go`. Highest-priority gap: R-NOT-004 (generation bump mid-roundtrip) — this was the Round-4 stranded-Symbol fix and currently has no synthetic regression test.
18. **Strengthen** `session_test.go` `TestZeroOldSymbolHandles` — extend to also assert Value, Valid, ValueParsed, LastUpdateTime are all cleared per R-CACHE-004.
19. **Add traceability comment header** to every test file per `04-functional-tests.md` convention. Each test fn gets a `// T-X-NNN — <one-liner> / Validates: R-...` block.
20. **Mark DP-1 / DP-3 missing tests** (R-NOT-016, R-NOT-017, R-CACHE-009..R-CACHE-018) as deferred — implementation pending per task #33. These are NOT release blockers today but must land alongside the DP-1 implementation.

---

## Summary

**Clean tests** (trace to spec, exercise behavior, would catch a real bug — graded A or solid B): approximately **84** of the **131** test functions audited.

**Section 2 — flagged tests: 10**
- Category A (mimics implementation): 3 — TestWindowsFiletimeConversion, TestSumNotificationResultTriState, TestClient_DialFailsWhenServerUnreachable.
- Category B (coverage theater): 4 — TestProcessImageConstants (partial), TestEncodePacket_AllCommands, TestGetJSON_NumericOverflow, TestReturnCodeString_AllCategories.
- Category C (tautological): 3 — TestSumReadOverflowGuard, TestProcessImageBitOffset, TestReturnCodeError_ImplementsError.
- Category D (unused/removed code path): 0 — all flagged tests do exercise extant code, just inadequately.

Plus ~37 NO-SPEC entries that are real regression guards but lack a requirement to anchor them (Section 4 items 9–11 propose three R-SYM/R-SUM backfills covering most of those).

**Section 3 — requirements with no test (gap count): ~50** of ~95 requirements.
- Heaviest gaps: R-CACHE-009..018 (DP-1/DP-3, all 10 — implementation pending), R-VIEW-001..007 (6 of 7), R-RECON-002..010 (6 of 9), R-NOT-001..017 (10 of 17), R-CL-001..008 (4 of 8 partial), R-SES-001..011 (8 of 11 partial).
- Doc-content tests entirely missing: T-DOC-001..004, T-U-1100.

Verdict: parsing/codec coverage is strong (R-PARSE-* mostly green); session/client/cache/notification/reconnect coverage is thin and is the biggest pre-1.0 risk. Phase 5 split (Client + Session) is largely untested at unit level — TestClient_* tests cover Dial+Close basics but not the per-invoke multiplexing, on-drop callback, post-Close error path, or goroutine bounds.
