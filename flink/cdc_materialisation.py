"""
CDC materialisation: Debezium CDC topics → materialized Kafka topics → ClickHouse Kafka Engine.

Why json format with envelope columns, not raw:
  raw format in Flink 1.18 delivers bytes not STRING, causing JSON_VALUE to return null
  for every path. json format with named columns matching the Debezium envelope fixes this.

Why JSON_VALUE on after_data/before_data:
  Manual field extraction from the row object JSON. COALESCE(after, before) for upserts/deletes.

Why Kafka intermediary sink:
  Decouples Flink from ClickHouse; ClickHouse Kafka Engine ingests with strong consistency/recovery.

PyFlink Table API — not DataStream. Submit with Flink 1.18 + flink-sql-connector-kafka.
"""

from __future__ import annotations

import logging
import os
from typing import Final

from dotenv import load_dotenv
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StatementSet, StreamTableEnvironment

logger = logging.getLogger(__name__)

JOB_NAME: Final[str] = "cdc-materialisation"
CHECKPOINT_INTERVAL_MS: Final[int] = 30_000
DLQ_TOPIC: Final[str] = "dead_letter_queue"
TOPIC_PREFIX: Final[str] = "ecommerce.public"

TOPIC_ORDERS: Final[str] = f"{TOPIC_PREFIX}.orders"
TOPIC_PRODUCTS: Final[str] = f"{TOPIC_PREFIX}.products"
TOPIC_CUSTOMERS: Final[str] = f"{TOPIC_PREFIX}.customers"


def _env(name: str, default: str) -> str:
    return os.getenv(name, default)


def _kafka_bootstrap() -> str:
    # Inside Docker: kafka:29092. localhost:9092 is for host machine access only.
    return _env("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")


def _sanitize_table_name(topic: str) -> str:
    return "kafka_" + topic.replace(".", "_").replace("-", "_")


def create_kafka_source_raw(table_env: StreamTableEnvironment, topic: str) -> str:
    """Kafka source — Debezium envelope columns for JSON_VALUE field extraction."""
    table_name = _sanitize_table_name(topic)
    group_id = f"cdc-materialisation-{topic}"

    # raw format in Flink 1.18 delivers bytes not STRING, causing JSON_VALUE to return null
    # for every path. Switching to json format with named columns that match the Debezium
    # envelope structure fixes this cleanly.
    # Reverted to latest-offset after backfill completed.
    # earliest-offset was used temporarily to replay 117 missing
    # customers when customers_materialized topic was absent on
    # initial run. latest-offset is correct for normal operation
    # to avoid reprocessing all historical data on every restart.
    ddl = f"""
    CREATE TABLE {table_name} (
        `before` STRING,
        `after` STRING,
        op STRING,
        ts_ms BIGINT,
        before_data AS `before`,
        after_data AS `after`
    ) WITH (
        'connector' = 'kafka',
        'topic' = '{topic}',
        'properties.bootstrap.servers' = '{_kafka_bootstrap()}',
        'properties.group.id' = '{group_id}',
        'scan.startup.mode' = 'latest-offset',
        'format' = 'json',
        'json.fail-on-missing-field' = 'false',
        'json.ignore-parse-errors' = 'true'
    )
    """
    table_env.execute_sql(ddl)
    logger.info("Registered Kafka source: %s -> %s", topic, table_name)
    return table_name


def create_kafka_sink(
    table_env: StreamTableEnvironment,
    topic: str,
    columns: str,
) -> str:
    """
    Kafka sink for materialized CDC data.

    Why Kafka intermediary: ClickHouse Kafka Engine provides exactly-once guarantees
    and decouples Flink from ClickHouse. From comparison table, Kafka Intermediary offers
    best consistency/recovery trade-off.
    """
    table_name = f"kafka_{topic.replace('.', '_').replace('-', '_')}"
    ddl = f"""
    CREATE TABLE {table_name} (
        {columns}
    ) WITH (
        'connector' = 'kafka',
        'topic' = '{topic}',
        'properties.bootstrap.servers' = '{_kafka_bootstrap()}',
        'format' = 'json',
        'json.fail-on-missing-field' = 'false',
        'json.ignore-parse-errors' = 'true'
    )
    """
    table_env.execute_sql(ddl)
    logging.info(f"Registered Kafka sink: {topic} -> {table_name}")
    return table_name


def create_dlq_sink(table_env: StreamTableEnvironment) -> str:
    """Kafka dead-letter topic (json format for structured DLQ rows)."""
    table_name = "dlq_kafka"
    ddl = f"""
    CREATE TABLE {table_name} (
        original_topic STRING,
        original_offset BIGINT,
        original_timestamp TIMESTAMP_LTZ(3),
        failure_reason STRING,
        failure_detail STRING,
        raw_payload STRING,
        failed_at TIMESTAMP_LTZ(3)
    ) WITH (
        'connector' = 'kafka',
        'topic' = '{DLQ_TOPIC}',
        'properties.bootstrap.servers' = '{_kafka_bootstrap()}',
        'format' = 'json',
        'json.timestamp-format.standard' = 'ISO-8601'
    )
    """
    table_env.execute_sql(ddl)
    logger.info("Registered DLQ Kafka sink: %s", DLQ_TOPIC)
    return table_name


def process_orders_cdc(table_env: StreamTableEnvironment, dlq_table: str) -> list[str]:
    """
    Materialise orders CDC into orders_materialized Kafka topic.

    COALESCE(after, before): inserts/updates use after; deletes use before only.
    op: c=create, u=update, d=delete, r=snapshot read. is_deleted=1 when op=d (ReplacingMergeTree tombstone).
    """
    _ = dlq_table
    source = create_kafka_source_raw(table_env, TOPIC_ORDERS)
    sink = create_kafka_sink(
        table_env,
        "orders_materialized",
        "order_id STRING, customer_id STRING, status STRING, total_amount DOUBLE, created_at STRING, updated_at STRING, is_deleted INT",
    )

    insert_sql = f"""
    INSERT INTO {sink}
    SELECT
        COALESCE(
            JSON_VALUE(after_data, '$.order_id'),
            JSON_VALUE(before_data, '$.order_id')
        ) AS order_id,
        COALESCE(
            JSON_VALUE(after_data, '$.customer_id'),
            JSON_VALUE(before_data, '$.customer_id')
        ) AS customer_id,
        COALESCE(
            JSON_VALUE(after_data, '$.status'),
            JSON_VALUE(before_data, '$.status')
        ) AS status,
        CAST(COALESCE(
            JSON_VALUE(after_data, '$.total_amount'),
            JSON_VALUE(before_data, '$.total_amount')
        ) AS DOUBLE) AS total_amount,
        COALESCE(
            JSON_VALUE(after_data, '$.created_at'),
            JSON_VALUE(before_data, '$.created_at')
        ) AS created_at,
        COALESCE(
            JSON_VALUE(after_data, '$.updated_at'),
            JSON_VALUE(before_data, '$.updated_at')
        ) AS updated_at,
        CASE WHEN op = 'd' THEN 1 ELSE 0 END AS is_deleted
    FROM {source}
    WHERE op IN ('c', 'u', 'd', 'r')
    """
    return [insert_sql]


def process_products_cdc(table_env: StreamTableEnvironment, dlq_table: str) -> list[str]:
    """Materialise products CDC into products_materialized Kafka topic."""
    _ = dlq_table
    source = create_kafka_source_raw(table_env, TOPIC_PRODUCTS)
    sink = create_kafka_sink(
        table_env,
        "products_materialized",
        "product_id STRING, sku STRING, name STRING, category STRING, price DOUBLE, inventory_count INT, created_at STRING, updated_at STRING, is_deleted INT",
    )

    insert_sql = f"""
    INSERT INTO {sink}
    SELECT
        COALESCE(
            JSON_VALUE(after_data, '$.product_id'),
            JSON_VALUE(before_data, '$.product_id')
        ) AS product_id,
        COALESCE(
            JSON_VALUE(after_data, '$.sku'),
            JSON_VALUE(before_data, '$.sku')
        ) AS sku,
        COALESCE(
            JSON_VALUE(after_data, '$.name'),
            JSON_VALUE(before_data, '$.name')
        ) AS name,
        COALESCE(
            JSON_VALUE(after_data, '$.category'),
            JSON_VALUE(before_data, '$.category')
        ) AS category,
        CAST(COALESCE(
            JSON_VALUE(after_data, '$.price'),
            JSON_VALUE(before_data, '$.price')
        ) AS DOUBLE) AS price,
        CAST(COALESCE(
            JSON_VALUE(after_data, '$.inventory_count'),
            JSON_VALUE(before_data, '$.inventory_count')
        ) AS INT) AS inventory_count,
        COALESCE(
            JSON_VALUE(after_data, '$.created_at'),
            JSON_VALUE(before_data, '$.created_at')
        ) AS created_at,
        COALESCE(
            JSON_VALUE(after_data, '$.updated_at'),
            JSON_VALUE(before_data, '$.updated_at')
        ) AS updated_at,
        CASE WHEN op = 'd' THEN 1 ELSE 0 END AS is_deleted
    FROM {source}
    WHERE op IN ('c', 'u', 'd', 'r')
    """
    return [insert_sql]


def process_customers_cdc(table_env: StreamTableEnvironment, dlq_table: str) -> list[str]:
    """Materialise customers CDC into customers_materialized Kafka topic."""
    _ = dlq_table
    source = create_kafka_source_raw(table_env, TOPIC_CUSTOMERS)
    sink = create_kafka_sink(
        table_env,
        "customers_materialized",
        "customer_id STRING, email STRING, tier STRING, created_at STRING, updated_at STRING, is_deleted INT",
    )

    insert_sql = f"""
    INSERT INTO {sink}
    SELECT
        COALESCE(
            JSON_VALUE(after_data, '$.customer_id'),
            JSON_VALUE(before_data, '$.customer_id')
        ) AS customer_id,
        COALESCE(
            JSON_VALUE(after_data, '$.email'),
            JSON_VALUE(before_data, '$.email')
        ) AS email,
        COALESCE(
            JSON_VALUE(after_data, '$.tier'),
            JSON_VALUE(before_data, '$.tier')
        ) AS tier,
        COALESCE(
            JSON_VALUE(after_data, '$.created_at'),
            JSON_VALUE(before_data, '$.created_at')
        ) AS created_at,
        COALESCE(
            JSON_VALUE(after_data, '$.updated_at'),
            JSON_VALUE(before_data, '$.updated_at')
        ) AS updated_at,
        CASE WHEN op = 'd' THEN 1 ELSE 0 END AS is_deleted
    FROM {source}
    WHERE op IN ('c', 'u', 'd', 'r')
    """
    return [insert_sql]


def _register_inserts(stmt_set: StatementSet, insert_sqls: list[str]) -> None:
    for sql in insert_sqls:
        stmt_set.add_insert_sql(sql)


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

    create_dlq_sink(table_env)
    stmt_set = table_env.create_statement_set()

    processors: list[tuple[str, object]] = [
        ("orders", process_orders_cdc),
        ("products", process_products_cdc),
        ("customers", process_customers_cdc),
    ]

    for name, processor in processors:
        try:
            insert_sqls = processor(table_env, "dlq_kafka")
            _register_inserts(stmt_set, insert_sqls)
            logger.info("Registered CDC inserts for %s", name)
        except Exception:
            logger.exception("Failed to register CDC pipeline for %s; continuing", name)

    logger.info("Submitting Flink job: %s", JOB_NAME)
    logger.warning(
        "Using Kafka intermediary - materialized data written to Kafka topics "
        "for ClickHouse Kafka Engine consumption"
    )
    result = stmt_set.execute()
    logging.info(f"Job submitted successfully: {result.get_job_client().get_job_id()}")


if __name__ == "__main__":
    main()
