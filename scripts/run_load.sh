#!/usr/bin/env bash
# scripts/run_load.sh
#
# Runs the load generator and optionally the benchmark scripts.
#
# Modes:
#   normal     — 10 TPS for a configurable duration (default 120s).
#                60% new orders, 30% status updates, 10% tier changes.
#
#   flash_sale — 50 TPS for 300s, targeting the 20 lowest-inventory products.
#                Includes 5% hard cancellations (DELETE + inventory restore).
#                After load completes, re-runs dbt with the correct flash_sale
#                start/end vars so mart_flash_sale_analysis flags these orders
#                correctly, then prints flash_sale_analysis.py output.
#
#   benchmark  — Full latency benchmark suite (~20 min). Runs normal load in
#                the background while measuring Postgres→Kafka and
#                Postgres→ClickHouse latency, then flash sale e2e. Saves
#                results to benchmarks/*.json and prints a summary table.
#
# Usage:
#   ./scripts/run_load.sh normal
#   ./scripts/run_load.sh normal --duration 300
#   ./scripts/run_load.sh flash_sale
#   ./scripts/run_load.sh benchmark

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")/.." && pwd)"

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BOLD='\033[1m'
NC='\033[0m'

log()     { echo -e "${GREEN}[load]${NC} $*"; }
warn()    { echo -e "${YELLOW}[load]${NC} $*"; }
fail()    { echo -e "${RED}[load]${NC} $*"; exit 1; }
section() { echo -e "\n${BOLD}── $* ──────────────────────────────────────────${NC}"; }

MODE=""
DURATION=120

while [[ $# -gt 0 ]]; do
  case "$1" in
    normal|flash_sale|benchmark)
      MODE="$1"
      shift
      ;;
    --duration)
      shift
      DURATION="$1"
      shift
      ;;
    *)
      echo "Unknown argument: $1"
      echo "Usage: ./scripts/run_load.sh <normal|flash_sale|benchmark> [--duration SECONDS]"
      exit 1
      ;;
  esac
done

if [[ -z "$MODE" ]]; then
  echo "Usage: ./scripts/run_load.sh <normal|flash_sale|benchmark> [--duration SECONDS]"
  exit 1
fi

# ── Activate virtualenv if needed ────────────────────────────────────────────
if [[ -z "${VIRTUAL_ENV:-}" ]] && [[ -f "$SCRIPT_DIR/.venv/bin/activate" ]]; then
  # shellcheck source=/dev/null
  source "$SCRIPT_DIR/.venv/bin/activate"
fi

# ── Verify pipeline is running before generating load ────────────────────────
if ! curl -sf http://localhost:8083/connectors/postgres-cdc-connector/status > /dev/null 2>&1; then
  fail "Debezium connector not reachable. Run ./start.sh first."
fi

CONNECTOR_STATE=$(curl -sf http://localhost:8083/connectors/postgres-cdc-connector/status \
  | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['connector']['state'])" 2>/dev/null || echo "UNKNOWN")

if [[ "$CONNECTOR_STATE" != "RUNNING" ]]; then
  fail "Debezium connector is $CONNECTOR_STATE, not RUNNING."
fi

# ── Load .env so Python scripts and dbt can read all vars ────────────────────
set -a
# shellcheck source=/dev/null
source "$SCRIPT_DIR/.env"
set +a

# ── Run mode ─────────────────────────────────────────────────────────────────
case "$MODE" in

  normal)
    section "Normal load — 10 TPS for ${DURATION}s"
    log "Watch the pipeline at: http://localhost:3001 (Grafana)"
    echo ""
    python3 "$SCRIPT_DIR/database/load_generator.py" normal --duration "$DURATION"
    log "Normal load complete."

    sleep 5
    COUNT=$(docker compose exec -T clickhouse \
      clickhouse-client --query \
      "SELECT count() FROM orders_current FINAL WHERE is_deleted=0" 2>/dev/null || echo "N/A")
    log "ClickHouse orders_current (live): $COUNT rows"
    ;;

  flash_sale)
    section "Flash sale — 50 TPS for 300s"
    warn "This takes 5 minutes and generates flash sale analytical data."
    warn "Watch inventory drain live: http://localhost:3001 → flash_sale_ops dashboard"
    echo ""

    # Record the start time before running — used as the dbt var
    FLASH_START=$(date -u +"%Y-%m-%d %H:%M:%S")
    log "Flash sale start time (UTC): $FLASH_START"

    python3 "$SCRIPT_DIR/database/load_generator.py" flash_sale

    FLASH_END=$(date -u +"%Y-%m-%d %H:%M:%S")
    log "Flash sale end time (UTC): $FLASH_END"
    log "Flash sale complete. Waiting 15s for pipeline to process the burst..."
    sleep 15

    # Re-run dbt with flash sale vars so mart_flash_sale_analysis correctly
    # flags which orders were placed during the flash sale window.
    # Without these vars, is_flash_sale_order stays 0 for all orders and
    # the slowdown_factor column returns NULL.
    log "Refreshing dbt gold models with flash sale vars..."
    cd "$SCRIPT_DIR/dbt"
    dbt run \
      --vars "{flash_sale_start: '${FLASH_START}', flash_sale_end: '${FLASH_END}'}" \
      --quiet
    dbt test \
      --vars "{flash_sale_start: '${FLASH_START}', flash_sale_end: '${FLASH_END}'}" \
      --quiet \
      || warn "Some dbt tests still failing — check output above."
    cd "$SCRIPT_DIR"

    section "Flash sale analysis"
    python3 "$SCRIPT_DIR/analysis/flash_sale_analysis.py"

    log ""
    log "To reprint at any time: python3 analysis/flash_sale_analysis.py"
    log "Flash sale vars used:   flash_sale_start='${FLASH_START}' flash_sale_end='${FLASH_END}'"
    log "Add these to .env or dbt_project.yml vars block to persist them."
    ;;

  benchmark)
    section "Full benchmark suite (~20 minutes)"
    warn "Do not interrupt — partial runs produce incomplete JSON files."
    echo ""

    # Verify CLICKHOUSE_NATIVE_PORT is set — e2e benchmark uses native protocol
    if [[ -z "${CLICKHOUSE_NATIVE_PORT:-}" ]]; then
      warn "CLICKHOUSE_NATIVE_PORT not set in .env. Defaulting to 19000."
      warn "If e2e benchmark times out, check your docker-compose.yml port mapping."
      export CLICKHOUSE_NATIVE_PORT=19000
    fi

    # ── Stage 2: Postgres → Kafka latency ────────────────────────────────────
    log "Stage 2: Postgres → Kafka latency (1000 samples at 10 TPS)..."
    python3 "$SCRIPT_DIR/database/load_generator.py" normal --duration 700 &
    LOAD_PID=$!
    sleep 5
    python3 "$SCRIPT_DIR/benchmarks/latency_test.py" \
      --samples 1000 \
      --load-condition normal
    kill "$LOAD_PID" 2>/dev/null || true
    wait "$LOAD_PID" 2>/dev/null || true
    log "Stage 2 complete. Sleeping 10s before next run..."
    sleep 10

    # ── Stage 3: Postgres → ClickHouse e2e latency — normal ──────────────────
    log "Stage 3: Postgres → ClickHouse e2e latency — normal load (500 samples)..."
    python3 "$SCRIPT_DIR/database/load_generator.py" normal --duration 700 &
    LOAD_PID=$!
    sleep 5
    python3 "$SCRIPT_DIR/benchmarks/e2e_latency_test.py" \
      --samples 500 \
      --mode normal
    kill "$LOAD_PID" 2>/dev/null || true
    wait "$LOAD_PID" 2>/dev/null || true
    log "Normal e2e complete. Sleeping 10s..."
    sleep 10

    # ── Stage 3: Postgres → ClickHouse e2e latency — flash sale ──────────────
    log "Stage 3: Postgres → ClickHouse e2e latency — flash sale (200 samples)..."
    python3 "$SCRIPT_DIR/database/load_generator.py" flash_sale &
    FLASH_PID=$!
    sleep 5
    python3 "$SCRIPT_DIR/benchmarks/e2e_latency_test.py" \
      --samples 200 \
      --mode flash_sale
    wait "$FLASH_PID" 2>/dev/null || true

    # ── Print results ─────────────────────────────────────────────────────────
    section "Benchmark results"
    python3 "$SCRIPT_DIR/benchmarks/print_results.py"

    log "JSON files saved to benchmarks/*.json"
    log "Commit them: git add benchmarks/*.json && git commit -m 'measured benchmark results'"
    ;;

  *)
    fail "Unknown mode: '$MODE'. Use: normal | flash_sale | benchmark"
    ;;
esac
