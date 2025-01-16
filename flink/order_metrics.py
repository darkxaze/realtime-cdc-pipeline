"""
Per-minute order KPIs: Debezium orders CDC → order_metrics_materialized → ClickHouse Kafka Engine.

Why tumbling windows, not sliding:
  Tumbling emits one non-overlapping row per minute bucket — correct for per-minute dashboards.
  Sliding windows overlap (same event in multiple buckets), making per-minute aggregation ambiguous.

Why 1-minute window size:
  Matches order_metrics_per_minute schema and Grafana grain; captures flash-sale spikes at 50 TPS
  without the state cost of sub-minute windows or hiding spikes in coarser buckets.

Why Kafka sink (order_metrics_materialized):
  Same intermediary pattern as cdc_materialisation.py — ClickHouse Kafka Engine consumes JSON
  rows and decouples Flink from the analytics sink.

PyFlink Table API — raw format + JSON_VALUE, not debezium-json. Flink 1.18 + flink-sql-connector-kafka.
"""

from __future__ import annotations

import logging
import os
from typing import Final

from dotenv import load_dotenv
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment

logger = logging.getLogger(__name__)

JOB_NAME: Final[str] = "order-metrics"
CHECKPOINT_INTERVAL_MS: Final[int] = 30_000
TOPIC_ORDERS: Final[str] = "ecommerce.public.orders"
TOPIC_METRICS: Final[str] = "order_metrics_materialized"
ORDERS_SOURCE_TABLE: Final[str] = "orders_kafka_raw"
METRICS_SINK_TABLE: Final[str] = "kafka_order_metrics_materialized"


def _env(name: str, default: str) -> str:
    return os.getenv(name, default)


def _kafka_bootstrap() -> str:
    # Inside Docker: kafka:29092. localhost:9092 is for host machine access only.
    return _env("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")


def create_orders_source(table_env: StreamTableEnvironment) -> str:
    """
    Raw Kafka source with PROCTIME() rowtime for tumbling windows.

    Using PROCTIME() not event time because:
    1. created_at is inside the JSON payload (STRING), not a top-level column
    2. Extracting it for watermarking requires a view, which cannot have WATERMARK
    3. PROCTIME() uses processing time — acceptable for per-minute aggregations
       where slight latency is acceptable. For exactly-correct window assignment
       use event time, but that requires Kafka timestamp or top-level time column.
    """
    ddl = f"""
    CREATE TABLE {ORDERS_SOURCE_TABLE} (
        payload STRING,
        event_time AS PROCTIME()
    ) WITH (
        'connector' = 'kafka',
        'topic' = '{TOPIC_ORDERS}',
        'properties.bootstrap.servers' = '{_kafka_bootstrap()}',
        'properties.group.id' = 'order-metrics-consumer',
        'scan.startup.mode' = 'latest-offset',
        'format' = 'raw'
    )
    """
    table_env.execute_sql(ddl)
    logger.info("Registered raw orders source: %s -> %s", TOPIC_ORDERS, ORDERS_SOURCE_TABLE)
    return ORDERS_SOURCE_TABLE


def create_metrics_kafka_sink(table_env: StreamTableEnvironment) -> str:
    """Kafka JSON sink for per-minute aggregates (ClickHouse Kafka Engine downstream)."""
    ddl = f"""
    CREATE TABLE {METRICS_SINK_TABLE} (
        window_start STRING,
        orders_count BIGINT,
        revenue DOUBLE,
        avg_order_value DOUBLE,
        cancellations_count BIGINT
    ) WITH (
        'connector' = 'kafka',
        'topic' = '{TOPIC_METRICS}',
        'properties.bootstrap.servers' = '{_kafka_bootstrap()}',
        'format' = 'json',
        'json.fail-on-missing-field' = 'false',
        'json.ignore-parse-errors' = 'true'
    )
    """
    table_env.execute_sql(ddl)
    logging.info("Registered Kafka metrics sink: %s -> %s", TOPIC_METRICS, METRICS_SINK_TABLE)
    return METRICS_SINK_TABLE


def compute_metrics(table_env: StreamTableEnvironment) -> str:
    """
    Tumbling 1-minute aggregation on PROCTIME() (processing-time windows).

    op IN ('c', 'u'): only creates and updates carry a meaningful after image for metrics.
    Deletes (op='d') must not be counted as cancelled orders in cancellations_count.
    """
    create_orders_source(table_env)
    create_metrics_kafka_sink(table_env)

    insert_sql = f"""
    INSERT INTO {METRICS_SINK_TABLE}
    SELECT
        CAST(TUMBLE_START(event_time, INTERVAL '1' MINUTE) AS STRING) AS window_start,
        COUNT(*) AS orders_count,
        COALESCE(SUM(
            CAST(COALESCE(
                JSON_VALUE(payload, '$.payload.after.total_amount'),
                JSON_VALUE(payload, '$.after.total_amount')
            ) AS DOUBLE)
        ), 0.0) AS revenue,
        COALESCE(AVG(
            CAST(COALESCE(
                JSON_VALUE(payload, '$.payload.after.total_amount'),
                JSON_VALUE(payload, '$.after.total_amount')
            ) AS DOUBLE)
        ), 0.0) AS avg_order_value,
        COUNT(CASE
            WHEN COALESCE(
                JSON_VALUE(payload, '$.payload.after.status'),
                JSON_VALUE(payload, '$.after.status')
            ) = 'cancelled' THEN 1
            ELSE NULL
        END) AS cancellations_count
    FROM {ORDERS_SOURCE_TABLE}
    WHERE COALESCE(
        JSON_VALUE(payload, '$.payload.op'),
        JSON_VALUE(payload, '$.op')
    ) IN ('c', 'u')
    GROUP BY TUMBLE(event_time, INTERVAL '1' MINUTE)
    """
    return insert_sql


def main() -> None:
    load_dotenv()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(CHECKPOINT_INTERVAL_MS)

    settings = EnvironmentSettings.in_streaming_mode()
    table_env = StreamTableEnvironment.create(env, environment_settings=settings)

    config = table_env.get_config().get_configuration()
    config.set_string("pipeline.name", JOB_NAME)
    config.set_string("execution.checkpointing.interval", str(CHECKPOINT_INTERVAL_MS))
    config.set_string("state.backend", "rocksdb")
    config.set_string("state.checkpoints.dir", "file:///tmp/flink-checkpoints")

    stmt_set = table_env.create_statement_set()
    stmt_set.add_insert_sql(compute_metrics(table_env))

    logger.info("Submitting Flink job: %s", JOB_NAME)
    result = stmt_set.execute()
    logging.info(f"Job submitted successfully: {result.get_job_client().get_job_id()}")


if __name__ == "__main__":
    main()
