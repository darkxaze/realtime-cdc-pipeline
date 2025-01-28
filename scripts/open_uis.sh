#!/usr/bin/env bash
# scripts/open_uis.sh
#
# Opens all pipeline UI tabs in the default browser.
# Can be run standalone at any time after the stack is up.
#
# UIs:
#   Flink UI        http://localhost:8082  — job status, checkpoints, task slots
#   Grafana         http://localhost:3001  — pipeline_health + flash_sale_ops dashboards
#   Kafka Connect   http://localhost:8083/connectors — connector REST API
#   ClickHouse Play http://localhost:8123/play — ad-hoc SQL query UI
#   Prometheus      http://localhost:9090  — raw metrics and target health
#   Airflow         http://localhost:8085  — DAG runs (only if --profile airflow is active)
#
# Usage: ./scripts/open_uis.sh

set -euo pipefail

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log()  { echo -e "${GREEN}[ui]${NC} $*"; }
warn() { echo -e "${YELLOW}[ui]${NC} $*"; }

open_browser() {
  local url="$1"
  if command -v xdg-open > /dev/null 2>&1; then
    xdg-open "$url" > /dev/null 2>&1 &   # Linux
  elif command -v open > /dev/null 2>&1; then
    open "$url"                           # macOS
  else
    warn "Cannot auto-open browser on this OS. Visit: $url"
  fi
}

# Detect actual Grafana host port from docker compose (handles port remapping)
GRAFANA_PORT=$(docker compose port grafana 3000 2>/dev/null | cut -d: -f2 || echo "3001")

echo ""
echo "  ┌─────────────────────────────────────────────────────────┐"
echo "  │  CDC Pipeline — Service URLs                             │"
echo "  ├─────────────────────────────────────────────────────────┤"
printf "  │  %-18s  http://localhost:8082%-12s│\n"  "Flink UI"       ""
printf "  │  %-18s  http://localhost:%-4s%-12s│\n"  "Grafana" "$GRAFANA_PORT" " (admin / admin)"
printf "  │  %-18s  http://localhost:8083/connectors%-3s│\n" "Kafka Connect"  ""
printf "  │  %-18s  http://localhost:8123/play%-8s│\n" "ClickHouse Play" ""
printf "  │  %-18s  http://localhost:9090%-12s│\n"  "Prometheus"     ""
echo "  └─────────────────────────────────────────────────────────┘"
echo ""

open_browser "http://localhost:8082"
sleep 0.4
open_browser "http://localhost:${GRAFANA_PORT}"
sleep 0.4
open_browser "http://localhost:8083/connectors"
sleep 0.4
open_browser "http://localhost:8123/play"
sleep 0.4
open_browser "http://localhost:9090"

# Only open Airflow if the webserver container is actually running
if docker compose ps airflow-webserver 2>/dev/null | grep -q "Up"; then
  printf "  │  %-18s  http://localhost:8085%-12s│\n" "Airflow" " (admin / admin)"
  open_browser "http://localhost:8085"
fi

log "All tabs opened."
