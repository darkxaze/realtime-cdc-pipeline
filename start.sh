#!/usr/bin/env bash
# start.sh
#
# Brings up the full CDC pipeline from a cold start.
#
# Steps (in order):
#   1.  Pre-flight       — docker, python, .env
#   2.  Build            — custom Flink image (bakes in JARs; skip with --no-build)
#   3.  Core services    — Postgres, Zookeeper, Kafka, Schema Registry, Debezium
#   4.  Health checks    — waits for each service to be healthy before proceeding
#   5.  Debezium         — cleans stale dbz_publication, registers connector, confirms RUNNING
#   6.  Analytics layer  — ClickHouse, Flink (jobmanager + taskmanager), Prometheus, Grafana
#   7.  Kafka topics     — creates intermediary topics + dead_letter_queue before Flink starts
#   8.  Database seed    — 1000 customers, 200 products (skipped if data already exists)
#   9.  CDC topics       — waits for all four Debezium topics before Flink submission
#  10.  Flink job        — submits cdc_materialisation.py
#  11.  MV backfill      — ensures order_metrics_per_minute is populated from existing data
#  12.  dbt              — deps + run + test
#  13.  Integrity check  — great_expectations/run_checkpoint.py
#  14.  Airflow          — optional: init DB, start services (requires --airflow flag)
#  15.  UIs              — opens browser tabs (suppress with --no-ui)
#
# Usage:
#   ./start.sh                   Full cold start (recommended for first run)
#   ./start.sh --no-seed         Skip seeding (data already exists)
#   ./start.sh --no-build        Skip docker compose build (image already built)
#   ./start.sh --airflow         Also initialise and start Airflow
#   ./start.sh --no-ui           Suppress opening browser tabs
#
# After the stack is running:
#   Normal load:      ./scripts/run_load.sh normal
#   Flash sale:       ./scripts/run_load.sh flash_sale
#   Benchmarks:       ./scripts/run_load.sh benchmark
#   Failure tests:    python3 tests/test_failure_recovery.py --test 1
#   Stop (keep data): ./stop.sh
#   Full reset:       ./stop.sh --clean && ./start.sh

set -euo pipefail

# ── Flags ────────────────────────────────────────────────────────────────────
SEED=true
BUILD=true
WITH_AIRFLOW=false
OPEN_UI=true

for arg in "$@"; do
  case $arg in
    --no-seed)  SEED=false ;;
    --no-build) BUILD=false ;;
    --airflow)  WITH_AIRFLOW=true ;;
    --no-ui)    OPEN_UI=false ;;
    --help|-h)
      grep "^# " "$0" | head -45 | sed 's/^# \?//'
      exit 0 ;;
    *)
      echo "Unknown argument: $arg  (run ./start.sh --help for usage)"
      exit 1 ;;
  esac
done

# ── Colour helpers ───────────────────────────────────────────────────────────
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BOLD='\033[1m'
NC='\033[0m'

log()     { echo -e "${GREEN}[start]${NC} $*"; }
warn()    { echo -e "${YELLOW}[start]${NC} $*"; }
fail()    { echo -e "${RED}[start]${NC} $*"; exit 1; }
section() { echo -e "\n${BOLD}── $* ──────────────────────────────────────────${NC}"; }

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/scripts/wait_healthy.sh"

# ── Header ────────────────────────────────────────────────────────────────────
echo ""
echo -e "${BOLD}  Real-time E-commerce CDC Pipeline${NC}"
echo    "  Starting full stack..."
[[ "$SEED" == false ]]      && echo "  [--no-seed]  skipping database seed"
[[ "$BUILD" == false ]]     && echo "  [--no-build] skipping image build"
[[ "$WITH_AIRFLOW" == true ]] && echo "  [--airflow]  Airflow will be initialised and started"
echo ""

# ═══════════════════════════════════════════════════════════════════════════
# STEP 1 — PRE-FLIGHT
# ═══════════════════════════════════════════════════════════════════════════
section "Pre-flight checks"

if ! docker info > /dev/null 2>&1; then
  fail "Docker is not running. Start Docker Desktop or: sudo systemctl start docker"
fi
log "Docker: ✓"

if ! docker compose version > /dev/null 2>&1; then
  fail "docker compose v2 not found. Update Docker Desktop or install the compose plugin."
fi
log "docker compose v2: ✓"

if [[ -z "${VIRTUAL_ENV:-}" ]] && [[ -f "$SCRIPT_DIR/.venv/bin/activate" ]]; then
  warn "Activating .venv..."
  # shellcheck source=/dev/null
  source "$SCRIPT_DIR/.venv/bin/activate"
fi
log "Python: $(python3 --version)"

if [[ ! -f "$SCRIPT_DIR/.env" ]]; then
  if [[ -f "$SCRIPT_DIR/.env.example" ]]; then
    warn ".env not found — copying from .env.example"
    cp "$SCRIPT_DIR/.env.example" "$SCRIPT_DIR/.env"
  else
    fail ".env not found. Copy .env.example to .env."
  fi
fi
log ".env: ✓"

# Load .env so all subsequent steps (dbt, Python scripts, port reads) work
set -a
# shellcheck source=/dev/null
source "$SCRIPT_DIR/.env"
set +a

# Make helper scripts executable
chmod +x \
  "$SCRIPT_DIR/debezium/connector_setup.sh" \
  "$SCRIPT_DIR/scripts/create_kafka_topics.sh" \
  "$SCRIPT_DIR/scripts/start_flink_jobs.sh" \
  "$SCRIPT_DIR/scripts/run_load.sh" \
  "$SCRIPT_DIR/scripts/open_uis.sh"

# ═══════════════════════════════════════════════════════════════════════════
# STEP 2 — BUILD CUSTOM FLINK IMAGE
# ═══════════════════════════════════════════════════════════════════════════
# flink/Dockerfile bakes in connector JARs (flink-sql-connector-kafka-3.0.2,
# clickhouse-jdbc-0.4.6-all, flink-metrics-prometheus) and Python packages.
# Without this image the jobs fail with ClassNotFound and GuavaClassNotFound
# errors that are hard to diagnose.

if [[ "$BUILD" == true ]]; then
  section "Building custom Flink image"
  log "Building flink-jobmanager and flink-taskmanager images..."
  log "(First build ~3-5 min; subsequent builds use Docker layer cache)"
  docker compose build flink-jobmanager flink-taskmanager
  log "Flink image built. ✓"
else
  log "Skipping image build (--no-build)."
fi

# ═══════════════════════════════════════════════════════════════════════════
# STEP 3 — CORE SERVICES
# ═══════════════════════════════════════════════════════════════════════════
section "Starting core services"

log "Starting: postgres, zookeeper, kafka, schema-registry, debezium..."
docker compose up -d postgres zookeeper kafka schema-registry debezium

# ═══════════════════════════════════════════════════════════════════════════
# STEP 4 — WAIT FOR CORE SERVICES
# ═══════════════════════════════════════════════════════════════════════════
section "Waiting for core services"

# Postgres has no HTTP endpoint — use pg_isready via docker compose exec
printf "  Waiting for %-26s" "Postgres..."
until docker compose exec -T postgres \
  pg_isready -U "${POSTGRES_USER:-postgres}" -q 2>/dev/null; do
  printf "."; sleep 2
done
echo " ✓"

wait_healthy "Kafka Connect"     "http://localhost:8083"               120
wait_healthy "Schema Registry"   "http://localhost:8081/subjects"       90
wait_healthy "Debezium Connect"  "http://localhost:8083/connectors"    120

# ═══════════════════════════════════════════════════════════════════════════
# STEP 5 — DEBEZIUM CONNECTOR
# ═══════════════════════════════════════════════════════════════════════════
# Known issue from Stage 2 build: if the stack was previously run without
# --clean, the dbz_publication and replication slot persist in Postgres across
# container restarts. The connector registration fails with:
#   "A logical publication named 'dbz_publication' is already active and
#    cannot be altered."
#
# We clean these up here before registering. connector_setup.sh handles the
# connector-side idempotency (delete-if-exists before POST).
section "Debezium connector setup"

log "Cleaning up stale Postgres publication and replication slot (if any)..."
docker compose exec -T postgres \
  psql -U "${POSTGRES_USER:-postgres}" -d "${POSTGRES_DB:-ecommerce}" \
  -c "DROP PUBLICATION IF EXISTS dbz_publication;" \
  -c "SELECT pg_drop_replication_slot(slot_name)
      FROM pg_replication_slots
      WHERE slot_name = 'debezium';" \
  2>/dev/null || true
# The replication slot query returns 0 rows and exits cleanly if the slot
# doesn't exist — the || true guard covers the case where the slot name
# differs from 'debezium' in older connector versions.

log "Registering Debezium connector..."
"$SCRIPT_DIR/debezium/connector_setup.sh"

# ═══════════════════════════════════════════════════════════════════════════
# STEP 6 — ANALYTICS LAYER
# ═══════════════════════════════════════════════════════════════════════════
section "Starting analytics layer"

# ClickHouse Kafka Engine tables attempt broker connection on startup. Even with
# depends_on: kafka: condition: service_healthy, there is a brief window after the
# healthcheck passes where the internal listener (kafka:29092) is not yet accepting
# connections. This causes ClickHouse to exit 215 on first boot. Retrying after
# 10 seconds resolves it consistently.
ANALYTICS_SERVICES=(clickhouse flink-jobmanager flink-taskmanager prometheus grafana)
MAX_ANALYTICS_ATTEMPTS=3
analytics_attempt=0
analytics_started=false

while [[ $analytics_attempt -lt $MAX_ANALYTICS_ATTEMPTS ]]; do
  analytics_attempt=$((analytics_attempt + 1))

  if [[ $analytics_attempt -eq 1 ]]; then
    log "Starting: clickhouse, flink-jobmanager, flink-taskmanager, prometheus, grafana (attempt ${analytics_attempt}/${MAX_ANALYTICS_ATTEMPTS})..."
    docker compose up -d clickhouse flink-jobmanager flink-taskmanager prometheus grafana
  else
    log "Retrying analytics layer (attempt ${analytics_attempt}/${MAX_ANALYTICS_ATTEMPTS})..."
    docker compose up -d
  fi

  all_up=true
  for svc in "${ANALYTICS_SERVICES[@]}"; do
    if ! docker compose ps "$svc" 2>/dev/null | grep -qE 'Up'; then
      all_up=false
      break
    fi
  done

  clickhouse_bad=false
  if docker compose ps clickhouse 2>/dev/null | grep -qE 'Exit|exited|Error'; then
    clickhouse_bad=true
  fi

  if [[ "$all_up" == true ]] && [[ "$clickhouse_bad" == false ]]; then
    analytics_started=true
    log "Analytics layer started successfully on attempt ${analytics_attempt}/${MAX_ANALYTICS_ATTEMPTS}."
    break
  fi

  if [[ $analytics_attempt -lt $MAX_ANALYTICS_ATTEMPTS ]] && [[ "$clickhouse_bad" == true ]]; then
    warn "ClickHouse is in Exit/Error state — waiting 10s before retry..."
    sleep 10
  fi
done

if [[ "$analytics_started" != true ]]; then
  echo ""
  echo -e "${RED}[start]${NC} Analytics layer failed to start after ${MAX_ANALYTICS_ATTEMPTS} attempts."
  echo -e "${RED}[start]${NC} ClickHouse logs (last 30 lines):"
  docker compose logs clickhouse --tail 30
  echo ""
  fail "Likely a Kafka timing issue: ClickHouse Kafka Engine connected before kafka:29092 was ready. Run: ./start.sh --no-build --no-seed to retry."
fi

section "Waiting for analytics services"

wait_healthy "ClickHouse"  "http://localhost:8123/ping"        90
wait_healthy "Flink UI"    "http://localhost:8082/overview"    90
wait_healthy "Grafana"     "http://localhost:3001/api/health"  90

# ═══════════════════════════════════════════════════════════════════════════
# STEP 7 — KAFKA TOPICS
# ═══════════════════════════════════════════════════════════════════════════
# Must happen BEFORE Flink job submission. Flink writes to these immediately
# on start. Missing topics = silent zero output (hit during Stage 3 build).
section "Creating Kafka topics"

"$SCRIPT_DIR/scripts/create_kafka_topics.sh"

# ═══════════════════════════════════════════════════════════════════════════
# STEP 8 — DATABASE SEED
# ═══════════════════════════════════════════════════════════════════════════
if [[ "$SEED" == true ]]; then
  section "Seeding database"
  REFERENCE_CDC_EMIT=false

  EXISTING=$(docker compose exec -T postgres \
    psql -U "${POSTGRES_USER:-postgres}" -d "${POSTGRES_DB:-ecommerce}" \
    -tAc "SELECT COUNT(*) FROM customers;" 2>/dev/null | tr -d '[:space:]' || echo "0")

  if [[ "$EXISTING" -gt 0 ]]; then
    warn "Database already has $EXISTING customers — skipping seed."
    warn "Use --no-seed to suppress this check on future starts."
  else
    log "Inserting 1000 customers and 200 products..."
    python3 "$SCRIPT_DIR/database/load_generator.py" seed
    log "Seed complete."

    # trigger Debezium to create the CDC topic for this table — Debezium creates topics lazily on first row change.
    log "Inserting one order to trigger ecommerce.public.orders topic..."
    docker compose exec -T postgres \
      psql -U "${POSTGRES_USER:-postgres}" -d "${POSTGRES_DB:-ecommerce}" \
      -c "INSERT INTO orders (customer_id, status, total_amount) SELECT customer_id, 'pending', 99.99 FROM customers LIMIT 1;"

    # trigger Debezium to create the CDC topic for this table — Debezium creates topics lazily on first row change.
    log "Inserting one order_item to trigger ecommerce.public.order_items topic..."
    docker compose exec -T postgres \
      psql -U "${POSTGRES_USER:-postgres}" -d "${POSTGRES_DB:-ecommerce}" \
      -c "INSERT INTO order_items (order_id, product_id, quantity, unit_price)
          SELECT o.order_id, p.product_id, 1, p.price
          FROM (SELECT order_id FROM orders ORDER BY created_at DESC LIMIT 1) o
          CROSS JOIN (SELECT product_id, price FROM products ORDER BY random() LIMIT 1) p;"

    REFERENCE_CDC_EMIT=true
  fi
else
  log "Skipping seed (--no-seed)."
fi

# ═══════════════════════════════════════════════════════════════════════════
# STEP 9 — WAIT FOR DEBEZIUM CDC TOPICS
# ═══════════════════════════════════════════════════════════════════════════
# Debezium creates CDC topics lazily on first row change per table. The seed
# inserts customers and products but not orders or order_items. Flink fails with
# UnknownTopicOrPartitionException if it starts before all four topics exist.
# The fix is to insert one row into orders and order_items during seed (or wait
# here and let normal load generator trigger the missing topics). We wait here
# because modifying seed behaviour would change Stage 1 behaviour.
#
# Kafka topic state is stored in the Kafka data volume which is not preserved
# between restarts (only postgres_data and clickhouse_data volumes persist). CDC
# topics must be recreated by triggering row changes in Postgres on every restart,
# even on --no-seed runs. These trigger inserts add a small number of extra orders
# to ClickHouse but do not affect the analytical results meaningfully.
CDC_TOPICS=(
  "ecommerce.public.orders"
  "ecommerce.public.customers"
  "ecommerce.public.products"
  "ecommerce.public.order_items"
)

_check_cdc_topics() {
  cdc_topic_list=$(docker compose exec -T kafka \
    kafka-topics --bootstrap-server localhost:9092 --list 2>/dev/null || true)
  cdc_missing=()
  cdc_existing=()
  for topic in "${CDC_TOPICS[@]}"; do
    if echo "$cdc_topic_list" | grep -q "^${topic}$"; then
      cdc_existing+=("$topic")
    else
      cdc_missing+=("$topic")
    fi
  done
}

_run_resume_trigger_inserts() {
  log "CDC topics missing on resume — running minimal trigger inserts to recreate them..."
  docker compose exec -T postgres \
    psql -U "${POSTGRES_USER:-postgres}" -d "${POSTGRES_DB:-ecommerce}" \
    -c "INSERT INTO orders (customer_id, status, total_amount) SELECT customer_id, 'pending', 99.99 FROM customers LIMIT 1;"

  docker compose exec -T postgres \
    psql -U "${POSTGRES_USER:-postgres}" -d "${POSTGRES_DB:-ecommerce}" \
    -c "INSERT INTO order_items (order_id, product_id, quantity, unit_price)
        SELECT o.order_id, p.product_id, 1, p.price
        FROM orders o CROSS JOIN products p
        WHERE o.status = 'pending'
        LIMIT 1;"
}

_wait_for_cdc_topics() {
  local timeout="$1"
  local elapsed=0
  cdc_topics_ready=false

  printf "  Waiting for CDC topics..."
  while [[ $elapsed -lt $timeout ]]; do
    _check_cdc_topics

    if [[ ${#cdc_missing[@]} -eq 0 ]]; then
      cdc_topics_ready=true
      echo " ✓"
      log "All four Debezium CDC topics exist."
      return 0
    fi

    printf "."
    sleep 3
    elapsed=$((elapsed + 3))
  done

  echo ""
  return 1
}

section "Waiting for Debezium CDC topics"

_check_cdc_topics
if [[ "$SEED" == false ]] && [[ ${#cdc_missing[@]} -gt 0 ]]; then
  _run_resume_trigger_inserts
fi

if ! _wait_for_cdc_topics 120; then
  if [[ "$SEED" == false ]]; then
    _check_cdc_topics
    if [[ ${#cdc_missing[@]} -gt 0 ]]; then
      _run_resume_trigger_inserts
      _wait_for_cdc_topics 60 || true
    fi
  fi
fi

if [[ "${cdc_topics_ready:-false}" != true ]]; then
  _check_cdc_topics
  echo -e "${RED}[start]${NC} Timed out waiting for Debezium CDC topics."
  if [[ ${#cdc_existing[@]} -gt 0 ]]; then
    echo -e "${RED}[start]${NC} Topics present: ${cdc_existing[*]}"
  else
    echo -e "${RED}[start]${NC} Topics present: (none)"
  fi
  if [[ ${#cdc_missing[@]} -gt 0 ]]; then
    echo -e "${RED}[start]${NC} Topics still missing: ${cdc_missing[*]}"
  fi
  fail "Flink cannot start until all four CDC topics exist. Check Debezium connector status: curl http://localhost:8083/connectors/postgres-cdc-connector/status"
fi

# ═══════════════════════════════════════════════════════════════════════════
# STEP 10 — FLINK JOB
# ═══════════════════════════════════════════════════════════════════════════
section "Submitting Flink job"

"$SCRIPT_DIR/scripts/start_flink_jobs.sh"

# These must run after Flink reaches RUNNING state. Flink uses latest-offset so any CDC events
# emitted before job submission are missed. The trigger inserts for orders/order_items run
# before Flink starts only to ensure those Kafka topics exist (Debezium creates topics lazily),
# not to populate ClickHouse.
if [[ "${REFERENCE_CDC_EMIT:-false}" == true ]]; then
  log "Emitting CDC events for all seeded customers and products (post-Flink-start)..."
  docker compose exec -T postgres \
    psql -U "${POSTGRES_USER:-postgres}" \
    -d "${POSTGRES_DB:-ecommerce}" \
    -c "UPDATE customers SET tier=tier;"

  docker compose exec -T postgres \
    psql -U "${POSTGRES_USER:-postgres}" \
    -d "${POSTGRES_DB:-ecommerce}" \
    -c "UPDATE products SET inventory_count=inventory_count;"
fi

# Wait for Flink to materialise reference data into ClickHouse before dbt reads it.
printf "  Waiting for customers_current in ClickHouse..."
CH_CUSTOMERS_TIMEOUT=120
ch_customers_elapsed=0
ch_customers_ready=false

while [[ $ch_customers_elapsed -lt $CH_CUSTOMERS_TIMEOUT ]]; do
  CH_CUSTOMER_COUNT=$(docker compose exec -T clickhouse \
    clickhouse-client --query "SELECT count() FROM customers_current FINAL" \
    2>/dev/null | tr -d '[:space:]' || echo "0")

  if [[ "$CH_CUSTOMER_COUNT" -ge 1000 ]]; then
    ch_customers_ready=true
    echo " ✓"
    log "customers_current: ${CH_CUSTOMER_COUNT} rows"
    break
  fi

  printf "."
  sleep 5
  ch_customers_elapsed=$((ch_customers_elapsed + 5))
done

if [[ "$ch_customers_ready" != true ]]; then
  echo ""
  warn "Timed out after ${CH_CUSTOMERS_TIMEOUT}s waiting for customers_current >= 1000 (got ${CH_CUSTOMER_COUNT:-0}). Continuing to dbt..."
fi

# ═══════════════════════════════════════════════════════════════════════════
# STEP 11 — ORDER METRICS MV BACKFILL
# ═══════════════════════════════════════════════════════════════════════════
# order_metrics_per_minute is populated by the ClickHouse Materialized View
# (order_metrics_mv) which fires on every INSERT to orders_current. On a
# fresh stack, the MV only covers inserts that arrive after it was created —
# any rows that landed before (e.g. from the Debezium initial snapshot) are
# not in order_metrics_per_minute yet.
#
# This one-time backfill query inserts the missing minute-level aggregates.
# It is idempotent: ClickHouse MergeTree deduplicates on (window_start).
section "Backfilling order_metrics_per_minute"

log "Running backfill for order_metrics_per_minute..."
docker compose exec -T clickhouse \
  clickhouse-client --query "
    INSERT INTO order_metrics_per_minute
    SELECT
      toStartOfMinute(created_at) AS window_start,
      countIf(is_deleted = 0)     AS orders_count,
      sumIf(total_amount, is_deleted = 0)                            AS revenue,
      avgIf(total_amount, is_deleted = 0)                            AS avg_order_value,
      countIf(status = 'cancelled' AND is_deleted = 0)               AS cancellations_count
    FROM orders_current FINAL
    GROUP BY toStartOfMinute(created_at)
    HAVING count() > 0
  " 2>/dev/null || warn "Backfill skipped — orders_current may be empty (expected on first run before load)."

log "Backfill complete."

# ═══════════════════════════════════════════════════════════════════════════
# STEP 12 — DBT
# ═══════════════════════════════════════════════════════════════════════════
# .env is already loaded (Step 1). dbt reads CLICKHOUSE_HOST from the shell
# environment — it does not auto-load .env itself.
section "Running dbt"

cd "$SCRIPT_DIR/dbt"

log "Installing dbt packages (dbt deps)..."
dbt deps --quiet

log "Running dbt models (staging + gold)..."
dbt run

log "Running dbt tests..."
# Two tests require flash sale data: assert_flash_sale_orders_exist and
# not_null_mart_flash_sale_analysis_flash_sale_avg_confirm_seconds.
# Both will fail here and pass automatically after ./scripts/run_load.sh flash_sale
dbt test || warn "Some dbt tests failed — expected until flash sale load is run (./scripts/run_load.sh flash_sale)."

cd "$SCRIPT_DIR"

# ═══════════════════════════════════════════════════════════════════════════
# STEP 13 — DATA INTEGRITY CHECK
# ═══════════════════════════════════════════════════════════════════════════
section "Data integrity check"

log "Running Great Expectations checkpoint (Postgres vs ClickHouse row count)..."
python3 "$SCRIPT_DIR/great_expectations/run_checkpoint.py" \
  || warn "Integrity check failed — may be transient if pipeline is still catching up. Re-run: python3 great_expectations/run_checkpoint.py"

# ═══════════════════════════════════════════════════════════════════════════
# STEP 14 — AIRFLOW (optional)
# ═══════════════════════════════════════════════════════════════════════════
# Airflow requires its own database and schema initialisation on first run.
# Stage 5 build notes: the airflow database must exist in Postgres before
# airflow db init, and airflow db upgrade is required after version changes.
# We handle all of this here so --airflow is truly one-flag setup.

if [[ "$WITH_AIRFLOW" == true ]]; then
  section "Setting up Airflow"

  log "Creating 'airflow' database in Postgres (if it doesn't exist)..."
  docker compose exec -T postgres \
    psql -U "${POSTGRES_USER:-postgres}" \
    -c "SELECT 1 FROM pg_database WHERE datname='airflow'" \
    | grep -q 1 \
    || docker compose exec -T postgres \
       psql -U "${POSTGRES_USER:-postgres}" \
       -c "CREATE DATABASE airflow;" 2>/dev/null
  log "Airflow database ready. ✓"

  log "Starting Airflow services (profile: airflow)..."
  docker compose --profile airflow up -d

  wait_healthy "Airflow Webserver" "http://localhost:8085/health" 120

  # Run db init/upgrade inside the scheduler container — idempotent
  log "Initialising Airflow metadata DB..."
  docker compose exec -T airflow-scheduler \
    airflow db upgrade 2>/dev/null || \
  docker compose exec -T airflow-scheduler \
    airflow db init 2>/dev/null || true

  # Create admin user if it doesn't already exist
  log "Creating Airflow admin user (admin/admin)..."
  docker compose exec -T airflow-webserver \
    airflow users create \
    --username admin \
    --password admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    2>/dev/null || warn "Admin user may already exist — skipping creation."

  log "Airflow is running. DAG: cdc_pipeline_dag (5-minute schedule)"
  log "Airflow UI: http://localhost:8085  (admin / admin)"
fi

# ═══════════════════════════════════════════════════════════════════════════
# STEP 15 — OPEN UIs
# ═══════════════════════════════════════════════════════════════════════════
if [[ "$OPEN_UI" == true ]]; then
  section "Opening dashboards"
  "$SCRIPT_DIR/scripts/open_uis.sh"
fi

# ═══════════════════════════════════════════════════════════════════════════
# DONE
# ═══════════════════════════════════════════════════════════════════════════
GRAFANA_PORT=$(docker compose port grafana 3000 2>/dev/null | cut -d: -f2 || echo "3001")

section "Stack is ready"
echo ""
echo -e "  ${GREEN}Flink UI${NC}         http://localhost:8082"
echo -e "  ${GREEN}Grafana${NC}          http://localhost:${GRAFANA_PORT}  (admin / admin)"
echo -e "  ${GREEN}Kafka Connect${NC}    http://localhost:8083/connectors"
echo -e "  ${GREEN}ClickHouse Play${NC}  http://localhost:8123/play"
echo -e "  ${GREEN}Prometheus${NC}       http://localhost:9090"
[[ "$WITH_AIRFLOW" == true ]] && echo -e "  ${GREEN}Airflow${NC}          http://localhost:8085  (admin / admin)"
echo ""
echo -e "  ${BOLD}Next steps:${NC}"
echo    "    See data flow:     ./scripts/run_load.sh normal"
echo    "    Flash sale data:   ./scripts/run_load.sh flash_sale"
echo    "    Full benchmarks:   ./scripts/run_load.sh benchmark"
echo    "    Failure tests:     python3 tests/test_failure_recovery.py --test 1"
echo    "    Integrity check:   python3 great_expectations/run_checkpoint.py"
echo    "    Stop (keep data):  ./stop.sh"
echo    "    Full reset:        ./stop.sh --clean"
echo ""
