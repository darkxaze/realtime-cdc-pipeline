#!/usr/bin/env bash
# scripts/wait_healthy.sh
#
# Shared utility — polls a URL until HTTP 200 or timeout.
# Source this file; do not run it directly.
#
# Usage: wait_healthy <label> <url> <timeout_seconds>

set -euo pipefail

wait_healthy() {
  local label="$1"
  local url="$2"
  local timeout="${3:-120}"
  local elapsed=0

  printf "  Waiting for %-26s" "$label..."

  while true; do
    if curl -sf --max-time 2 "$url" > /dev/null 2>&1; then
      echo " ✓"
      return 0
    fi

    if [[ $elapsed -ge $timeout ]]; then
      echo " ✗ timed out after ${timeout}s"
      echo ""
      echo "  ERROR: $label did not become healthy."
      echo "  Check logs with: docker compose logs ${label}"
      return 1
    fi

    printf "."
    sleep 2
    elapsed=$((elapsed + 2))
  done
}

export -f wait_healthy
