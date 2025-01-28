#!/usr/bin/env bash
# scripts/start_flink_jobs.sh
#
# Submits the CDC materialisation Flink job.
#
# Why only one job: order_metrics.py was superseded during Stage 3 by a
# ClickHouse Materialized View (order_metrics_mv) which aggregates directly
# from orders_current on every INSERT. The MV is more reliable than Flink
# PROCTIME() tumbling windows under bursty load — windows only close when a
# new event arrives after the boundary, so idle periods silently skip windows.
# The Flink order_metrics.py is kept in the repo for reference and future
# event-time windowing work (see future_work.md), but is not submitted here.
#
# This script can be run standalone after a container restart — the custom
# Flink Docker image bakes in all JARs, but Flink job state does not persist
# across container restarts unless a checkpoint is available.
#
# Usage: ./scripts/start_flink_jobs.sh

set -euo pipefail

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

log()  { echo -e "${GREEN}[flink]${NC} $*"; }
warn() { echo -e "${YELLOW}[flink]${NC} $*"; }
fail() { echo -e "${RED}[flink]${NC} $*"; exit 1; }

# ── Check jobmanager is reachable ────────────────────────────────────────────
log "Checking Flink jobmanager..."
if ! curl -sf http://localhost:8082/overview > /dev/null; then
  fail "Flink jobmanager not reachable at http://localhost:8082. Is the stack running?"
fi

# ── Check for existing running jobs to avoid double-submission ───────────────
RUNNING_JOBS=$(docker compose exec flink-jobmanager \
  /opt/flink/bin/flink list 2>/dev/null | grep -c "RUNNING" || true)

if [[ "$RUNNING_JOBS" -gt 0 ]]; then
  warn "$RUNNING_JOBS job(s) already RUNNING. Skipping submission."
  warn "To resubmit: cancel jobs at http://localhost:8082, then re-run this script."
  docker compose exec flink-jobmanager /opt/flink/bin/flink list 2>/dev/null | grep "RUNNING" | sed 's/^/  /'
  exit 0
fi

# ── Submit CDC materialisation job ──────────────────────────────────────────
# This job reads from Debezium CDC topics (ecommerce.public.*), transforms
# raw JSON payloads using JSON_VALUE(), and writes flat JSON to the three
# intermediary Kafka topics consumed by ClickHouse Kafka Engine tables.
log "Submitting cdc_materialisation.py..."
docker compose exec flink-jobmanager \
  /opt/flink/bin/flink run \
  --detached \
  --python /opt/flink/jobs/cdc_materialisation.py

log "Job submitted. Waiting 8s for it to reach RUNNING state..."
sleep 8

# ── Verify job reached RUNNING state ─────────────────────────────────────────
RUNNING=$(docker compose exec flink-jobmanager \
  /opt/flink/bin/flink list 2>/dev/null | grep "RUNNING" || true)

if [[ -z "$RUNNING" ]]; then
  fail "Job submitted but not in RUNNING state after 8s. Check: http://localhost:8082"
fi

log "Flink job running:"
echo "$RUNNING" | sed 's/^/  /'
log "Flink UI: http://localhost:8082"
