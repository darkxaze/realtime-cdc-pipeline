"""
Stage 2 benchmark: Postgres commit → Kafka (Debezium) arrival latency on orders CDC.

Requires: docker compose up, connector registered, seeded customers/products optional
(only customer_id needed for order inserts).
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Sequence
from uuid import UUID, uuid4

import numpy as np
import psycopg2
from confluent_kafka import Consumer, KafkaError, KafkaException, TopicPartition
from dotenv import load_dotenv
from psycopg2.extensions import connection as PgConnection

logger = logging.getLogger(__name__)

# confluent-kafka wraps librdkafka (C): lower poll latency and production parity with
# Flink/Java clients. kafka-python is pure-Python and slower under tight poll loops.
TOPIC_ORDERS = "ecommerce.public.orders"
POLL_INTERVAL_SEC = 1.0
MESSAGE_TIMEOUT_SEC = 30.0
GAP_BETWEEN_SAMPLES_SEC = 0.5
OUTPUT_PATH = Path(__file__).resolve().parent / "stage2_latency.json"


@dataclass(frozen=True)
class BenchmarkConfig:
    samples: int
    load_condition: str
    gap_sec: float
    message_timeout_sec: float


@dataclass(frozen=True)
class LatencyResult:
    p50_ms: float | None
    p95_ms: float | None
    p99_ms: float | None
    sample_count: int
    timeout_count: int
    timestamp: str
    load_condition: str


def _env(name: str, default: str) -> str:
    return os.getenv(name, default)


def connect_postgres() -> PgConnection:
    return psycopg2.connect(
        host=_env("POSTGRES_HOST", "localhost"),
        port=int(_env("POSTGRES_PORT", "5434")),
        user=_env("POSTGRES_USER", "postgres"),
        password=_env("POSTGRES_PASSWORD", "postgres"),
        dbname=_env("POSTGRES_DB", "ecommerce"),
    )


def fetch_sample_customer_id(conn: PgConnection) -> UUID:
    with conn.cursor() as cur:
        cur.execute("SELECT customer_id FROM customers ORDER BY RANDOM() LIMIT 1")
        row = cur.fetchone()
    if row is None:
        raise RuntimeError("No customers in database; run seed/load_generator first.")
    return UUID(str(row[0]))


def insert_order(conn: PgConnection, customer_id: UUID, order_id: UUID) -> None:
    with conn.cursor() as cur:
        cur.execute(
            (
                "INSERT INTO orders (order_id, customer_id, status, total_amount) "
                "VALUES (%s, %s, 'pending', %s)"
            ),
            (str(order_id), str(customer_id), "9.99"),
        )
    conn.commit()


def _parse_json_bytes(raw: bytes | None) -> Any | None:
    if raw is None:
        return None
    try:
        return json.loads(raw.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError):
        return None


def _order_id_from_key(key_data: Any) -> str | None:
    if isinstance(key_data, dict):
        oid = key_data.get("order_id")
        return str(oid) if oid is not None else None
    return None


def _order_id_from_value(value_data: Any) -> str | None:
    if not isinstance(value_data, dict):
        return None
    payload = value_data.get("payload")
    if not isinstance(payload, dict):
        return None
    if payload.get("op") not in ("c", "r"):
        return None
    after = payload.get("after")
    if not isinstance(after, dict):
        return None
    oid = after.get("order_id")
    return str(oid) if oid is not None else None


def extract_order_id(key: bytes | None, value: bytes | None) -> str | None:
    key_obj = _parse_json_bytes(key)
    value_obj = _parse_json_bytes(value)
    return _order_id_from_key(key_obj) or _order_id_from_value(value_obj)


def create_consumer() -> Consumer:
    group_id = f"latency-test-{uuid4()}"
    conf = {
        "bootstrap.servers": _env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        "group.id": group_id,
        "auto.offset.reset": "latest",
        "enable.auto.commit": False,
    }
    consumer = Consumer(conf)

    def on_assign(consumer: Consumer, partitions: list[TopicPartition]) -> None:
        for part in partitions:
            _, high = consumer.get_watermark_offsets(part, timeout=10.0)
            part.offset = high
        consumer.assign(partitions)

    consumer.subscribe([TOPIC_ORDERS], on_assign=on_assign)
    return consumer


def wait_for_assignment(consumer: Consumer, timeout_sec: float = 30.0) -> None:
    deadline = time.perf_counter() + timeout_sec
    while time.perf_counter() < deadline:
        consumer.poll(0.5)
        if consumer.assignment():
            return
    raise TimeoutError(f"Kafka consumer not assigned to {TOPIC_ORDERS} within {timeout_sec}s")


def poll_for_order(
    consumer: Consumer,
    target_order_id: str,
    timeout_sec: float,
) -> float | None:
    deadline = time.perf_counter() + timeout_sec
    while time.perf_counter() < deadline:
        msg = consumer.poll(POLL_INTERVAL_SEC)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            raise KafkaException(msg.error())
        found_id = extract_order_id(msg.key(), msg.value())
        if found_id == target_order_id:
            return time.perf_counter()
    return None


def run_benchmark(
    conn: PgConnection,
    consumer: Consumer,
    customer_id: UUID,
    cfg: BenchmarkConfig,
) -> tuple[list[float], int]:
    latencies_ms: list[float] = []
    timeout_count = 0

    for i in range(cfg.samples):
        order_id = uuid4()
        insert_order(conn, customer_id, order_id)
        t_commit = time.perf_counter()

        t_arrival = poll_for_order(consumer, str(order_id), cfg.message_timeout_sec)
        if t_arrival is None:
            timeout_count += 1
            logger.warning(
                "Sample %s/%s: no Kafka message for order_id=%s within %.0fs",
                i + 1,
                cfg.samples,
                order_id,
                cfg.message_timeout_sec,
            )
        else:
            latency_ms = (t_arrival - t_commit) * 1000.0
            latencies_ms.append(latency_ms)
            logger.debug(
                "Sample %s/%s: order_id=%s latency=%.2f ms",
                i + 1,
                cfg.samples,
                order_id,
                latency_ms,
            )

        if i < cfg.samples - 1:
            time.sleep(cfg.gap_sec)

    return latencies_ms, timeout_count


def compute_percentiles(latencies_ms: Sequence[float]) -> tuple[float | None, float | None, float | None]:
    if not latencies_ms:
        return None, None, None
    arr = np.asarray(latencies_ms, dtype=np.float64)
    p50, p95, p99 = np.percentile(arr, [50, 95, 99])
    return float(p50), float(p95), float(p99)


def save_results(result: LatencyResult) -> None:
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "p50_ms": result.p50_ms,
        "p95_ms": result.p95_ms,
        "p99_ms": result.p99_ms,
        "sample_count": result.sample_count,
        "timeout_count": result.timeout_count,
        "timestamp": result.timestamp,
        "load_condition": result.load_condition,
    }
    with OUTPUT_PATH.open("w", encoding="utf-8") as fh:
        json.dump(payload, fh, indent=2)
    logger.info("Wrote %s", OUTPUT_PATH)


def print_results_table(result: LatencyResult) -> None:
    def fmt_ms(value: float | None) -> str:
        return f"{value:.2f}" if value is not None else "n/a"

    logger.info("Stage 2 latency — Postgres commit → Kafka (%s)", TOPIC_ORDERS)
    logger.info("  load_condition   %s", result.load_condition)
    logger.info("  timestamp        %s", result.timestamp)
    logger.info("  sample_count     %s", result.sample_count)
    logger.info("  timeout_count    %s", result.timeout_count)
    logger.info("  p50_ms           %s", fmt_ms(result.p50_ms))
    logger.info("  p95_ms           %s", fmt_ms(result.p95_ms))
    logger.info("  p99_ms           %s", fmt_ms(result.p99_ms))
    logger.info("  output           %s", OUTPUT_PATH)


def parse_args() -> BenchmarkConfig:
    parser = argparse.ArgumentParser(
        description="Measure Postgres commit to Kafka CDC arrival latency (orders).",
    )
    parser.add_argument(
        "--samples",
        type=int,
        default=1000,
        help="Number of insert/poll measurements (default: 1000).",
    )
    parser.add_argument(
        "--load-condition",
        default="idle",
        help="Label for this run (e.g. idle, normal, flash_sale).",
    )
    parser.add_argument(
        "--gap",
        type=float,
        default=GAP_BETWEEN_SAMPLES_SEC,
        help="Seconds between samples (default: 0.5).",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=MESSAGE_TIMEOUT_SEC,
        help="Seconds to wait per sample for Kafka message (default: 30).",
    )
    args = parser.parse_args()
    if args.samples < 1:
        raise ValueError("--samples must be >= 1")
    return BenchmarkConfig(
        samples=args.samples,
        load_condition=args.load_condition,
        gap_sec=args.gap,
        message_timeout_sec=args.timeout,
    )


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    project_root = Path(__file__).resolve().parent.parent
    load_dotenv(project_root / ".env")

    cfg = parse_args()
    logger.info(
        "Starting latency benchmark: samples=%s load_condition=%s topic=%s",
        cfg.samples,
        cfg.load_condition,
        TOPIC_ORDERS,
    )

    conn = connect_postgres()
    consumer = create_consumer()
    try:
        wait_for_assignment(consumer)
        customer_id = fetch_sample_customer_id(conn)
        latencies_ms, timeout_count = run_benchmark(conn, consumer, customer_id, cfg)
    finally:
        consumer.close()
        conn.close()

    p50, p95, p99 = compute_percentiles(latencies_ms)
    result = LatencyResult(
        p50_ms=p50,
        p95_ms=p95,
        p99_ms=p99,
        sample_count=len(latencies_ms),
        timeout_count=timeout_count,
        timestamp=datetime.now(timezone.utc).isoformat(),
        load_condition=cfg.load_condition,
    )
    save_results(result)
    print_results_table(result)


if __name__ == "__main__":
    main()
