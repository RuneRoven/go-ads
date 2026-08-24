#!/bin/bash
# Run integration tests from Docker container (bridge mode, NOT host mode).
# This validates route registration works when container IP differs from host IP.
#
# Usage: docker/run-tests.sh [env-file] [test-pattern]
#   env-file:     Path to .env file (default: .env.integration.224)
#   test-pattern: Test regex (default: core tests + DockerRoute)

set -euo pipefail

# Colors (terminal only)
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
CYAN='\033[0;36m'
DIM='\033[2m'
BOLD='\033[1m'
RESET='\033[0m'

# Regex patterns (must be unquoted variables for bash =~)
RE_RUN='^=== RUN   (.+)'
RE_PASS='^--- PASS: (.+) \(([^)]+)\)'
RE_FAIL='^--- FAIL: (.+) \(([^)]+)\)'
RE_SKIP='^--- SKIP: (.+) \(([^)]+)\)'
RE_TLOG='^    .+\.go:[0-9]+: (.+)'
RE_WARN='^[0-9]{4}/[0-9]{2}/[0-9]{2}.* WARN (.+)'
RE_ERROR='^[0-9]{4}/[0-9]{2}/[0-9]{2}.* ERROR (.+)'
RE_INFO='^[0-9]{4}/[0-9]{2}/[0-9]{2}.* INFO (.+)'

ENV_FILE="${1:-.env.integration.224}"
TEST_PATTERN="${2:-TestIntegration(Connect|ReadDeviceInfo|ReadSymbol|Notification$|WriteAndConfirm|DockerRoute)}"

# Derive a label from the env file name
ENV_SUFFIX=$(basename "$ENV_FILE" | sed 's/\.env\.integration\.//')

if [[ ! -f "$ENV_FILE" ]]; then
    echo -e "${RED}Error: env file $ENV_FILE not found${RESET}"
    exit 1
fi

# shellcheck disable=SC1090
set -a && source "$ENV_FILE" && set +a

# Honor explicit ADS_HOST_IP override before autodetection (F-32).
HOST_IP="${ADS_HOST_IP:-}"
if [[ -z "$HOST_IP" ]]; then
    HOST_IP=$(ifconfig 2>/dev/null | grep "inet " | grep -v 127.0.0.1 | head -1 | awk '{print $2}' || true)
fi
if [[ -z "$HOST_IP" ]]; then
    HOST_IP=$(ip -4 addr show scope global 2>/dev/null | grep -oE 'inet [0-9.]+' | head -1 | awk '{print $2}' || true)
fi

if [[ -z "$HOST_IP" ]]; then
    echo -e "${RED}Error: could not detect host IP. Set ADS_HOST_IP manually.${RESET}"
    exit 1
fi

PLC_IP="${ADS_PLC_IP:-unknown}"

echo ""
echo -e "${BOLD}${CYAN}══════════════════════════════════════════════════════════════${RESET}"
echo -e "${BOLD}${CYAN}  Hardware Test: PLC ${PLC_IP} (.${ENV_SUFFIX})${RESET}"
echo -e "${CYAN}──────────────────────────────────────────────────────────────${RESET}"
echo -e "  Host IP:  ${HOST_IP}"
echo -e "  PLC IP:   ${PLC_IP}"
echo -e "  Env:      ${ENV_FILE}"
echo -e "${CYAN}══════════════════════════════════════════════════════════════${RESET}"
echo ""

# Build test image (quiet)
echo -e "${DIM}Building test image...${RESET}"
docker build -q -f docker/Dockerfile.test -t go-ads-test . > /dev/null
echo -e "${DIM}Build complete${RESET}"
echo ""

# Run tests, capture raw output
RUN_EXIT=0
RAW_OUTPUT=$(docker run --rm \
    --env-file "$ENV_FILE" \
    -e "ADS_HOST_IP=$HOST_IP" \
    go-ads-test \
    -test.v -test.timeout "${ADS_TEST_TIMEOUT:-120s}" \
    -test.run "$TEST_PATTERN" 2>&1) || RUN_EXIT=$?

# ── Format terminal output ──
format_terminal() {
    local current_parent=""

    while IFS= read -r line; do
        if [[ "$line" =~ $RE_RUN ]]; then
            local test_name="${BASH_REMATCH[1]}"
            if [[ "$test_name" != */* ]]; then
                [[ -n "$current_parent" ]] && echo ""
                current_parent="$test_name"
            fi
            continue
        fi

        if [[ "$line" =~ $RE_PASS ]]; then
            local name="${BASH_REMATCH[1]}" dur="${BASH_REMATCH[2]}"
            if [[ "$name" == */* ]]; then
                echo -e "    ${GREEN}✓${RESET} ${name##*/} ${DIM}(${dur})${RESET}"
            else
                echo -e "  ${GREEN}✓${RESET} ${BOLD}${name}${RESET} ${DIM}(${dur})${RESET}"
            fi
            continue
        fi

        if [[ "$line" =~ $RE_FAIL ]]; then
            local name="${BASH_REMATCH[1]}" dur="${BASH_REMATCH[2]}"
            if [[ "$name" == */* ]]; then
                echo -e "    ${RED}✗${RESET} ${name##*/} ${DIM}(${dur})${RESET}"
            else
                echo -e "  ${RED}✗${RESET} ${BOLD}${name}${RESET} ${DIM}(${dur})${RESET}"
            fi
            continue
        fi

        if [[ "$line" =~ $RE_SKIP ]]; then
            local name="${BASH_REMATCH[1]}"
            if [[ "$name" == */* ]]; then
                echo -e "    ${YELLOW}○${RESET} ${name##*/} ${DIM}(skipped)${RESET}"
            else
                echo -e "  ${YELLOW}○${RESET} ${BOLD}${name}${RESET} ${DIM}(skipped)${RESET}"
            fi
            continue
        fi

        if [[ "$line" =~ $RE_TLOG ]]; then
            echo -e "      ${DIM}${BASH_REMATCH[1]}${RESET}"
            continue
        fi

        if [[ "$line" =~ $RE_WARN ]]; then
            echo -e "      ${YELLOW}⚠ ${BASH_REMATCH[1]}${RESET}"
            continue
        fi

        if [[ "$line" =~ $RE_ERROR ]]; then
            echo -e "      ${RED}✗ ${BASH_REMATCH[1]}${RESET}"
            continue
        fi

        # INFO/DEBUG — suppress in terminal
    done
}

# ── Format log output (plain text, structured) ──
format_log() {
    local current_parent=""
    local test_num=0
    local log_lines=()

    echo "============================================================"
    echo "  Hardware Test Log: PLC ${PLC_IP} (.${ENV_SUFFIX})"
    echo "  Date: $(date '+%Y-%m-%d %H:%M:%S')"
    echo "  Host IP: ${HOST_IP}"
    echo "============================================================"
    echo ""

    flush_log_lines() {
        if [[ ${#log_lines[@]} -gt 0 ]]; then
            for ll in "${log_lines[@]}"; do
                echo "    $ll"
            done
            log_lines=()
        fi
    }

    while IFS= read -r line; do
        if [[ "$line" =~ $RE_RUN ]]; then
            local test_name="${BASH_REMATCH[1]}"
            if [[ "$test_name" != */* ]]; then
                flush_log_lines
                [[ -n "$current_parent" ]] && echo ""
                ((test_num++)) || true
                current_parent="$test_name"
                echo "------------------------------------------------------------"
                echo "  [$test_num] $test_name"
                echo "------------------------------------------------------------"
            fi
            continue
        fi

        if [[ "$line" =~ $RE_PASS ]]; then
            local name="${BASH_REMATCH[1]}" dur="${BASH_REMATCH[2]}"
            if [[ "$name" == */* ]]; then
                echo "    [PASS] ${name##*/}  (${dur})"
            else
                flush_log_lines
                echo "  => PASS  (${dur})"
            fi
            continue
        fi

        if [[ "$line" =~ $RE_FAIL ]]; then
            local name="${BASH_REMATCH[1]}" dur="${BASH_REMATCH[2]}"
            if [[ "$name" == */* ]]; then
                echo "    [FAIL] ${name##*/}  (${dur})"
            else
                flush_log_lines
                echo "  => FAIL  (${dur})"
            fi
            continue
        fi

        if [[ "$line" =~ $RE_SKIP ]]; then
            local name="${BASH_REMATCH[1]}"
            if [[ "$name" == */* ]]; then
                echo "    [SKIP] ${name##*/}"
            else
                flush_log_lines
                echo "  => SKIP"
            fi
            continue
        fi

        if [[ "$line" =~ $RE_TLOG ]]; then
            log_lines+=("${BASH_REMATCH[1]}")
            continue
        fi

        if [[ "$line" =~ $RE_WARN ]]; then
            log_lines+=("[WARN] ${BASH_REMATCH[1]}")
            continue
        fi

        if [[ "$line" =~ $RE_ERROR ]]; then
            log_lines+=("[ERROR] ${BASH_REMATCH[1]}")
            continue
        fi

        if [[ "$line" =~ $RE_INFO ]]; then
            log_lines+=("${BASH_REMATCH[1]}")
            continue
        fi
    done

    flush_log_lines
    echo ""
}

# ── Terminal output ──
echo "$RAW_OUTPUT" | format_terminal

# ── Parse summary ──
PASSED=$(echo "$RAW_OUTPUT" | grep -c "^--- PASS:" || true)
FAILED=$(echo "$RAW_OUTPUT" | grep -c "^--- FAIL:" || true)
SKIPPED=$(echo "$RAW_OUTPUT" | grep -c "^--- SKIP:" || true)
TOTAL=$((PASSED + FAILED + SKIPPED))

echo ""
echo -e "${CYAN}══════════════════════════════════════════════════════════════${RESET}"
echo -e "${BOLD}  Summary: PLC ${PLC_IP} (.${ENV_SUFFIX})${RESET}"
echo -e "${CYAN}──────────────────────────────────────────────────────────────${RESET}"
echo -e "  Total: ${BOLD}${TOTAL}${RESET}    ${GREEN}Pass: ${PASSED}${RESET}    ${RED}Fail: ${FAILED}${RESET}    ${YELLOW}Skip: ${SKIPPED}${RESET}"

if [[ "$FAILED" -gt 0 ]]; then
    echo ""
    echo -e "  ${RED}${BOLD}Failed:${RESET}"
    echo "$RAW_OUTPUT" | grep "^--- FAIL:" | while read -r fline; do
        echo -e "    ${RED}✗ ${fline#--- FAIL: }${RESET}"
    done
fi

echo -e "${CYAN}══════════════════════════════════════════════════════════════${RESET}"
echo ""

# ── Log output (structured plain text to stderr) ──
{
    echo "$RAW_OUTPUT" | format_log

    echo "============================================================"
    echo "  Summary"
    echo "============================================================"
    echo "  Total: ${TOTAL}    Pass: ${PASSED}    Fail: ${FAILED}    Skip: ${SKIPPED}"
    if [[ "$FAILED" -gt 0 ]]; then
        echo ""
        echo "  Failed:"
        echo "$RAW_OUTPUT" | grep "^--- FAIL:" | while read -r fline; do
            echo "    ✗ ${fline#--- FAIL: }"
        done
    fi
    echo "============================================================"
    echo ""
    echo ""
    echo "======================== RAW OUTPUT ========================"
    echo "$RAW_OUTPUT"
} >&2

if [[ "$RUN_EXIT" -ne 0 || "$FAILED" -gt 0 ]]; then
    exit 1
fi
exit 0
