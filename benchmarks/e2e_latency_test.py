"""
Stage 3 benchmark: Postgres commit → ClickHouse queryable row (orders_current).

Requires: docker compose up, Debezium connector, Flink cdc-materialisation job,
seeded customers, ClickHouse schema applied.

Measures wall-clock time from Postgres COMMIT until a FINAL query sees the row.
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
from clickhouse_driver import Client as ClickHouseClient
from dotenv import load_dotenv
from psycopg2.extensions import connection as PgConnection

logger = logging.getLogger(__name__)

# 50ms poll: tight enough to measure sub-second p50 without hammering ClickHouse at 20 QPS
# per sample (100ms would add up to ±50ms measurement jitter; 10ms would add merge load).
POLL_INTERVAL_MS: int = 50
POLL_INTERVAL_SEC: float = POLL_INTERVAL_MS / 1000.0
TIMEOUT_SEC: float = 10.0
GAP_BETWEEN_SAMPLES_SEC: float = 0.1
BENCHMARKS_DIR = Path(__file__).resolve().parent


@dataclass(frozen=True)
class BenchmarkConfig:
    samples: int
    mode: str
    gap_sec: float
    timeout_sec: float
    poll_interval_sec: float


@dataclass(frozen=True)
class E2ELatencyResult:
    p50_ms: float | None
    p95_ms: float | None
    p99_ms: float | None
    sample_count: int
    timeout_count: int
    timestamp: str
    mode: str
    polling_interval_ms: int


def _env(name: str, default: str) -> str:
    return os.getenv(name, default)


def output_path(mode: str) -> Path:
    return BENCHMARKS_DIR / f"stage3_e2e_latency_{mode}.json"


def connect_postgres() -> PgConnection:
    return psycopg2.connect(
        host=_env("POSTGRES_HOST", "localhost"),
        port=int(_env("POSTGRES_PORT", "5434")),
        user=_env("POSTGRES_USER", "postgres"),
        password=_env("POSTGRES_PASSWORD", "postgres"),
        dbname=_env("POSTGRES_DB", "ecommerce"),
    )


def connect_clickhouse() -> ClickHouseClient:
    # clickhouse-driver uses native protocol; host maps 19000 -> container 9000 (see docker-compose.yml).
    return ClickHouseClient(
        host=_env("CLICKHOUSE_HOST", "localhost"),
        port=int(_env("CLICKHOUSE_NATIVE_PORT", "19000")),
        user=_env("CLICKHOUSE_USER", "default"),
        password=_env("CLICKHOUSE_PASSWORD", ""),
        database=_env("CLICKHOUSE_DB", "default"),
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


def order_visible_in_clickhouse(
    client: ClickHouseClient,
    order_id: UUID,
) -> bool:
    """
    FINAL forces ReplacingMergeTree dedup before count(); without it, async merges
    can leave stale versions visible and under-report arrival (Bug 2 pattern).
    """
    rows = client.execute(
        (
            "SELECT count() FROM orders_current FINAL "
            "WHERE order_id = %(order_id)s AND is_deleted = 0"
        ),
        {"order_id": str(order_id)},
    )
    return bool(rows and rows[0][0] == 1)


def poll_clickhouse_for_order(
    client: ClickHouseClient,
    order_id: UUID,
    timeout_sec: float,
    poll_interval_sec: float,
) -> float | None:
    deadline = time.perf_counter() + timeout_sec
    while time.perf_counter() < deadline:
        if order_visible_in_clickhouse(client, order_id):
            return time.perf_counter()
        time.sleep(poll_interval_sec)
    return None


def run_benchmark(
    pg_conn: PgConnection,
    ch_client: ClickHouseClient,
    customer_id: UUID,
    cfg: BenchmarkConfig,
) -> tuple[list[float], int]:
    latencies_ms: list[float] = []
    timeout_count = 0

    for i in range(cfg.samples):
        order_id = uuid4()
        insert_order(pg_conn, customer_id, order_id)
        t_commit = time.perf_counter()

        t_arrival = poll_clickhouse_for_order(
            ch_client,
            order_id,
            cfg.timeout_sec,
            cfg.poll_interval_sec,
        )
        if t_arrival is None:
            timeout_count += 1
            logger.warning(
                "Sample %s/%s: order_id=%s not queryable in ClickHouse within %.0fs",
                i + 1,
                cfg.samples,
                order_id,
                cfg.timeout_sec,
            )
        else:
            latency_ms = (t_arrival - t_commit) * 1000.0
            latencies_ms.append(latency_ms)
            logger.debug(
                "Sample %s/%s: order_id=%s e2e_latency=%.2f ms",
                i + 1,
                cfg.samples,
                order_id,
                latency_ms,
            )

        if i < cfg.samples - 1:
            time.sleep(cfg.gap_sec)

    return latencies_ms, timeout_count


def compute_percentiles(
    latencies_ms: Sequence[float],
) -> tuple[float | None, float | None, float | None]:
    if not latencies_ms:
        return None, None, None
    arr = np.asarray(latencies_ms, dtype=np.float64)
    p50, p95, p99 = np.percentile(arr, [50, 95, 99])
    return float(p50), float(p95), float(p99)


def save_results(result: E2ELatencyResult, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload: dict[str, Any] = {
        "p50_ms": result.p50_ms,
        "p95_ms": result.p95_ms,
        "p99_ms": result.p99_ms,
        "sample_count": result.sample_count,
        "timeout_count": result.timeout_count,
        "timestamp": result.timestamp,
        "mode": result.mode,
        "polling_interval_ms": result.polling_interval_ms,
    }
    with path.open("w", encoding="utf-8") as fh:
        json.dump(payload, fh, indent=2)
    logger.info("Wrote %s", path)


def print_results_table(result: E2ELatencyResult, path: Path) -> None:
    def fmt_ms(value: float | None) -> str:
        return f"{value:.2f}" if value is not None else "n/a"

    logger.info("Stage 3 E2E latency — Postgres commit → ClickHouse queryable")
    logger.info("  mode               %s", result.mode)
    logger.info("  timestamp          %s", result.timestamp)
    logger.info("  sample_count       %s", result.sample_count)
    logger.info("  timeout_count      %s", result.timeout_count)
    logger.info("  polling_interval_ms %s", result.polling_interval_ms)
    logger.info("  p50_ms             %s", fmt_ms(result.p50_ms))
    logger.info("  p95_ms             %s", fmt_ms(result.p95_ms))
    logger.info("  p99_ms             %s", fmt_ms(result.p99_ms))
    logger.info("  output             %s", path)


def parse_args() -> BenchmarkConfig:
    parser = argparse.ArgumentParser(
        description="Measure Postgres commit to ClickHouse queryable latency (orders_current).",
    )
    parser.add_argument(
        "--samples",
        type=int,
        default=1000,
        help="Number of insert/poll measurements (default: 1000).",
    )
    parser.add_argument(
        "--mode",
        choices=("normal", "flash_sale"),
        default="normal",
        help="Load label for output file (normal or flash_sale).",
    )
    parser.add_argument(
        "--gap",
        type=float,
        default=GAP_BETWEEN_SAMPLES_SEC,
        help="Seconds between samples (default: 0.1).",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=TIMEOUT_SEC,
        help="Seconds to wait per sample for ClickHouse row (default: 10).",
    )
    args = parser.parse_args()
    if args.samples < 1:
        raise ValueError("--samples must be >= 1")
    return BenchmarkConfig(
        samples=args.samples,
        mode=args.mode,
        gap_sec=args.gap,
        timeout_sec=args.timeout,
        poll_interval_sec=POLL_INTERVAL_SEC,
    )


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    project_root = Path(__file__).resolve().parent.parent
    load_dotenv(project_root / ".env")

    cfg = parse_args()
    out_path = output_path(cfg.mode)
    logger.info(
        "Starting E2E benchmark: samples=%s mode=%s poll_ms=%s timeout=%ss",
        cfg.samples,
        cfg.mode,
        POLL_INTERVAL_MS,
        cfg.timeout_sec,
    )

    pg_conn = connect_postgres()
    ch_client = connect_clickhouse()
    try:
        customer_id = fetch_sample_customer_id(pg_conn)
        latencies_ms, timeout_count = run_benchmark(pg_conn, ch_client, customer_id, cfg)
    finally:
        pg_conn.close()
        ch_client.disconnect()

    p50, p95, p99 = compute_percentiles(latencies_ms)
    result = E2ELatencyResult(
        p50_ms=p50,
        p95_ms=p95,
        p99_ms=p99,
        sample_count=len(latencies_ms),
        timeout_count=timeout_count,
        timestamp=datetime.now(timezone.utc).isoformat(),
        mode=cfg.mode,
        polling_interval_ms=POLL_INTERVAL_MS,
    )
    save_results(result, out_path)
    print_results_table(result, out_path)


if __name__ == "__main__":
    main()
