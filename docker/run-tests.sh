#!/bin/bash
# Run integration tests from Docker container (bridge mode, NOT host mode).
# This validates route registration works when container IP differs from host IP.
#
# Usage: docker/run-tests.sh [env-file] [test-pattern]
#   env-file:     Path to .env file (default: .env.integration.224)
#   test-pattern: Test regex (default: core tests + DockerRoute)

set -euo pipefail

ENV_FILE="${1:-.env.integration.224}"
TEST_PATTERN="${2:-TestIntegration(Connect|ReadDeviceInfo|ReadSymbol|Notification$|WriteAndConfirm|DockerRoute)}"

if [[ ! -f "$ENV_FILE" ]]; then
    echo "Error: env file $ENV_FILE not found"
    exit 1
fi

# shellcheck disable=SC1090
set -a && source "$ENV_FILE" && set +a

# Detect host IP for Docker bridge — PLC needs this to reach the container
# Try macOS first (ifconfig), then Linux (ip addr)
HOST_IP=$(ifconfig 2>/dev/null | grep "inet " | grep -v 127.0.0.1 | head -1 | awk '{print $2}' || true)
if [[ -z "$HOST_IP" ]]; then
    HOST_IP=$(ip -4 addr show scope global 2>/dev/null | grep -oE 'inet [0-9.]+' | head -1 | awk '{print $2}' || true)
fi

if [[ -z "$HOST_IP" ]]; then
    echo "Error: could not detect host IP. Set ADS_HOST_IP manually."
    exit 1
fi

echo "Host IP: $HOST_IP"
echo "PLC IP:  ${ADS_PLC_IP:-not set}"
echo "Tests:   $TEST_PATTERN"
echo "Env:     $ENV_FILE"
echo ""

# Build test image from repo root
docker build -f docker/Dockerfile.test -t go-ads-test .

# Run in bridge mode (NOT host) — container gets its own IP
docker run --rm \
    --env-file "$ENV_FILE" \
    -e "ADS_HOST_IP=$HOST_IP" \
    go-ads-test \
    -test.v -test.timeout 120s \
    -test.run "$TEST_PATTERN"
