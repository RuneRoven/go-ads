#!/bin/bash
# Run docker/run-tests.sh against several PLCs CONCURRENTLY.
#
# Each PLC is a physically separate device, so concurrent runs cannot collide on
# the PLC side. Tests against a SINGLE PLC are never parallelised here — one
# run-tests.sh invocation per PLC, exactly as the sequential path does it.
#
# That is a hard rule, not a style choice: two concurrent test runs against the
# same PLC have been measured to break its AMS router. Hence the duplicate check
# below, which refuses to start rather than fanning out twice at one device.
#
# Usage: docker/run-tests-parallel.sh [plc ...]
#   plc: PLC suffix (224, 118, 70) or a path to an env file.
#        Default: all three (224 118 70).
#
# Env:
#   TEST_PATTERN       -test.run regex, forwarded to run-tests.sh. Unset means
#                      run-tests.sh's own six-test smoke default, which does NOT
#                      cover batch, reconnect or route-force behaviour — pass
#                      'TestIntegration' for the whole tagged suite.
#   ADS_TEST_TIMEOUT   per-run go test timeout (run-tests.sh default 120s). The
#                      smoke set fits in 120s; the full suite does not.
#
# Output: each run is buffered into its own logs/hardware-<suffix>-<ts>.log so
# three concurrent runs never interleave on the terminal. A per-PLC summary is
# printed as each run finishes, then a final table.

set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(dirname "$SCRIPT_DIR")
cd "$REPO_ROOT"

RED='\033[0;31m'
GREEN='\033[0;32m'
CYAN='\033[0;36m'
DIM='\033[2m'
BOLD='\033[1m'
RESET='\033[0m'

POLL_INTERVAL=2

DEFAULT_PLCS=(224 118 70)
if [[ $# -gt 0 ]]; then
    ARGS=("$@")
else
    ARGS=("${DEFAULT_PLCS[@]}")
fi

# ── Resolve arguments to env files, fail before starting anything ──
SUFFIXES=()
ENV_FILES=()
PLC_IPS=()
for arg in "${ARGS[@]}"; do
    if [[ -f "$arg" ]]; then
        env_file="$arg"
    else
        env_file=".env.integration.${arg}"
    fi
    if [[ ! -f "$env_file" ]]; then
        echo -e "${RED}Error: env file $env_file not found${RESET}" >&2
        exit 1
    fi
    suffix=$(basename "$env_file" | sed 's/\.env\.integration\.//')
    # One run per PLC, always. Two concurrent runs against the same device break
    # its AMS router, so a duplicate is refused rather than quietly launched twice.
    #
    # Compared by ADS_PLC_IP, not by file name: two env files can differ in name
    # and target the same device (a copy made to vary route credentials, symbol
    # names or ports), and that is the case a name check waves straight through.
    plc_ip="$(
        set -a
        # shellcheck disable=SC1090 # path is a runtime argument
        . "$env_file"
        set +a
        printf '%s' "${ADS_PLC_IP:-}"
    )"
    if [[ -z "$plc_ip" ]]; then
        echo -e "${RED}Error: $env_file sets no ADS_PLC_IP${RESET}" >&2
        exit 1
    fi
    # Plain length check then index loop. `${!PLC_IPS[@]+...}` mixes index
    # expansion with the alternate-value form, expands to nothing, and silently
    # skips the comparison entirely — which is how an earlier version of this
    # guard let two concurrent runs at one PLC straight through.
    for ((i = 0; i < ${#PLC_IPS[@]}; i++)); do
        if [[ "${PLC_IPS[$i]}" == "$plc_ip" ]]; then
            echo -e "${RED}Error: $env_file and .env.integration.${SUFFIXES[$i]} both target ${plc_ip}${RESET}" >&2
            echo -e "${RED}       never run two test runs against one PLC concurrently — it breaks its AMS router${RESET}" >&2
            exit 1
        fi
    done
    ENV_FILES+=("$env_file")
    SUFFIXES+=("$suffix")
    PLC_IPS+=("$plc_ip")
done

TS=$(date +%Y%m%d-%H%M%S)
mkdir -p logs

STATUS_DIR=$(mktemp -d "${TMPDIR:-/tmp}/go-ads-parallel.XXXXXX")
trap 'rm -rf "$STATUS_DIR"' EXIT

# ── Build the test image ONCE, before fanning out ──
# run-tests.sh always rebuilds `go-ads-test` itself. That is left alone on
# purpose: the tag is stable and every run builds the identical context
# (docker/Dockerfile.test + repo root), so the concurrent rebuilds are
# no-op cache hits against an image this pre-build already materialised.
# Pre-building serialises the one expensive, cache-cold build so three
# concurrent cold builds cannot race on the same tag.
echo -e "${BOLD}${CYAN}Building test image go-ads-test (once)...${RESET}"
docker build -q -f docker/Dockerfile.test -t go-ads-test . > /dev/null
echo -e "${DIM}Build complete${RESET}"
echo ""

# ── Fan out ──
PIDS=()
LOGS=()
STATUS_FILES=()
for i in "${!ENV_FILES[@]}"; do
    suffix="${SUFFIXES[$i]}"
    log="logs/hardware-${suffix}-${TS}.log"
    status="${STATUS_DIR}/${suffix}.rc"
    LOGS+=("$log")
    STATUS_FILES+=("$status")

    (
        rc=0
        # TEST_PATTERN is forwarded positionally; unset means run-tests.sh's own
        # default. Quoting an empty second argument would override that default
        # with an empty regex, which matches nothing, so branch instead.
        if [[ -n "${TEST_PATTERN:-}" ]]; then
            docker/run-tests.sh "${ENV_FILES[$i]}" "$TEST_PATTERN" > "$log" 2>&1 || rc=$?
        else
            docker/run-tests.sh "${ENV_FILES[$i]}" > "$log" 2>&1 || rc=$?
        fi
        # Write-then-rename: the poll loop below tests for the file's existence,
        # and bash reads an empty $(cat) as 0 — i.e. PASS — so a half-written
        # status file would silently turn a failed run green.
        echo "$rc" > "${status}.partial"
        mv "${status}.partial" "$status"
    ) &
    PIDS+=("$!")
    echo -e "  ${CYAN}started${RESET} .${suffix}  ${DIM}-> ${log}${RESET}"
done
echo ""
echo -e "${DIM}Waiting for ${#PIDS[@]} run(s)...${RESET}"
echo ""

# Print the plain-text summary line run-tests.sh wrote into the log.
print_summary() {
    local suffix="$1" log="$2" rc="$3" label
    if [[ "$rc" -eq 0 ]]; then
        label="${GREEN}PASS${RESET}"
    else
        label="${RED}FAIL${RESET}"
    fi
    echo -e "${CYAN}────────────────────────────────────────────────────────${RESET}"
    echo -e "  ${BOLD}.${suffix}${RESET}  ${label}  ${DIM}(exit ${rc})${RESET}"
    # Strip ANSI, take the last "Total: ... Pass: ..." line from the log.
    local line
    line=$(sed $'s/\033\\[[0-9;]*m//g' "$log" | grep -E '^ +Total: [0-9]+ +Pass:' | tail -1 || true)
    if [[ -n "$line" ]]; then
        echo "  ${line# }"
    fi
    if [[ "$rc" -ne 0 ]]; then
        sed $'s/\033\\[[0-9;]*m//g' "$log" | grep '^--- FAIL:' | sort -u | while read -r fline; do
            echo -e "    ${RED}x ${fline#--- FAIL: }${RESET}"
        done
    fi
    echo -e "  ${DIM}log: ${log}${RESET}"
}

# ── Collect results as each run finishes ──
RCS=()
REPORTED=()
for i in "${!PIDS[@]}"; do
    RCS+=(-1)
    REPORTED+=(0)
done

remaining=${#PIDS[@]}
while [[ "$remaining" -gt 0 ]]; do
    for i in "${!PIDS[@]}"; do
        if [[ "${REPORTED[$i]}" -eq 1 ]]; then
            continue
        fi
        if [[ ! -f "${STATUS_FILES[$i]}" ]]; then
            continue
        fi
        rc=$(cat "${STATUS_FILES[$i]}")
        RCS[i]="$rc"
        REPORTED[i]=1
        remaining=$((remaining - 1))
        print_summary "${SUFFIXES[$i]}" "${LOGS[$i]}" "$rc"
    done
    if [[ "$remaining" -gt 0 ]]; then
        sleep "$POLL_INTERVAL"
    fi
done

# Reap the subshells; status already recorded above.
for pid in "${PIDS[@]}"; do
    wait "$pid" || true
done

# ── Final table ──
EXIT_CODE=0
echo ""
echo -e "${BOLD}${CYAN}════════════════════════════════════════════════════════${RESET}"
echo -e "${BOLD}  Hardware Test Results${RESET}"
echo -e "${CYAN}────────────────────────────────────────────────────────${RESET}"
for i in "${!SUFFIXES[@]}"; do
    if [[ "${RCS[$i]}" -eq 0 ]]; then
        printf "  %-8s ${GREEN}%-6s${RESET} %s\n" ".${SUFFIXES[$i]}" "PASS" "${LOGS[$i]}"
    else
        printf "  %-8s ${RED}%-6s${RESET} %s\n" ".${SUFFIXES[$i]}" "FAIL" "${LOGS[$i]}"
        EXIT_CODE=1
    fi
done
echo -e "${BOLD}${CYAN}════════════════════════════════════════════════════════${RESET}"
echo ""

exit "$EXIT_CODE"
