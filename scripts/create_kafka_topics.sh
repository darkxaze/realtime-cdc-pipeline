#!/usr/bin/env bash
# scripts/create_kafka_topics.sh
#
# Creates all Kafka topics that must exist before Flink jobs are submitted
# and before the DLQ consumer can run.
#
# Two groups:
#
# INTERMEDIARY TOPICS (Flink → ClickHouse Kafka Engine):
#   orders_materialized        — flattened order CDC events written by Flink
#   products_materialized      — flattened product CDC events written by Flink
#   customers_materialized     — flattened customer CDC events written by Flink
#   order_metrics_materialized — reserved for future Flink event-time windowing
#                                (currently superseded by ClickHouse MV)
#
# DLQ TOPIC (Flink → DLQ consumer):
#   dead_letter_queue          — events that failed sink/validation in Flink,
#                                consumed by dlq/dlq_consumer.py for alerting
#                                and replay
#
# Why pre-create: Kafka auto-creation is disabled for CDC topics. Flink produces
# to these topics immediately on job start. If they don't exist, the job runs at
# LAG=0 with zero output — one of the hardest failure modes to diagnose in this
# stack (hit during Stage 3 build).
#
# Idempotent: safe to re-run. Existing topics are skipped.
#
# Usage: ./scripts/create_kafka_topics.sh

set -euo pipefail

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log()  { echo -e "${GREEN}[topics]${NC} $*"; }
warn() { echo -e "${YELLOW}[topics]${NC} $*"; }

TOPICS=(
  "orders_materialized"
  "products_materialized"
  "customers_materialized"
  "order_metrics_materialized"
  "dead_letter_queue"
)

log "Creating Kafka topics..."

for topic in "${TOPICS[@]}"; do
  EXISTS=$(docker compose exec -T kafka \
    kafka-topics --bootstrap-server localhost:9092 \
    --list 2>/dev/null | grep -c "^${topic}$" || true)

  if [[ "$EXISTS" -gt 0 ]]; then
    warn "Already exists, skipping: $topic"
  else
    docker compose exec -T kafka \
      kafka-topics \
      --bootstrap-server localhost:9092 \
      --create \
      --topic "$topic" \
      --partitions 1 \
      --replication-factor 1

    log "Created: $topic"
  fi
done

log "All topics ready."
log "Topic list (CDC + intermediary + DLQ):"
docker compose exec -T kafka \
  kafka-topics --bootstrap-server localhost:9092 --list \
  | grep -E "(ecommerce|materialized|dead_letter)" \
  | sed 's/^/  /'
