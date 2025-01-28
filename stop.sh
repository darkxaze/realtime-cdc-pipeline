#!/usr/bin/env bash
# stop.sh
#
# Shuts down the CDC pipeline stack.
#
# Usage:
#   ./stop.sh           # Stop containers, preserve data volumes
#   ./stop.sh --clean   # Stop containers AND delete all data volumes
#
# Without --clean, Postgres, ClickHouse, and Flink checkpoint data survive.
# The next ./start.sh --no-seed --no-build picks up exactly where you left off.
#
# With --clean, all volumes are wiped. The next ./start.sh runs a completely
# fresh pipeline from zero — useful when you want to reproduce results from scratch
# or recover from a corrupted state.

set -euo pipefail

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BOLD='\033[1m'
NC='\033[0m'

log()  { echo -e "${GREEN}[stop]${NC} $*"; }
warn() { echo -e "${YELLOW}[stop]${NC} $*"; }

CLEAN=false
for arg in "$@"; do
  case $arg in
    --clean) CLEAN=true ;;
    --help|-h)
      grep "^# " "$0" | head -20 | sed 's/^# \?//'
      exit 0
      ;;
    *) echo "Unknown argument: $arg"; exit 1 ;;
  esac
done

echo ""
echo -e "${BOLD}  Stopping CDC Pipeline...${NC}"
echo ""

if [[ "$CLEAN" == true ]]; then
  warn "--clean: this will DELETE all data volumes."
  warn "  - Postgres: all tables, seed data, CDC history"
  warn "  - ClickHouse: all materialised tables and metrics"
  warn "  - Flink: all checkpoints"
  warn ""
  warn "Press Ctrl-C within 5 seconds to cancel."
  sleep 5
  echo ""

  log "Stopping containers and removing volumes..."
  # Stop both default and airflow profile services
  docker compose --profile airflow down --volumes 2>/dev/null \
    || docker compose down --volumes

  log "All containers stopped. All data volumes removed."
  log "Next run: ./start.sh  (full cold start)"
else
  log "Stopping containers (volumes preserved)..."
  docker compose --profile airflow down 2>/dev/null \
    || docker compose down

  log "All containers stopped. Data volumes are intact."
  log "Resume with: ./start.sh --no-seed --no-build"
fi

echo ""
