"""
Great Expectations–style data quality checkpoint for the CDC pipeline.

Runs ClickHouse column checks and a 24-hour Postgres vs ClickHouse row-count
integrity check. Exit 0 on full pass, 1 on any failure.
"""

from __future__ import annotations

import logging
import os
import sys

import psycopg2
from clickhouse_driver import Client as ClickHouseClient
from dotenv import load_dotenv
from psycopg2.extensions import connection as PgConnection

logger = logging.getLogger(__name__)

ACCEPTED_ORDER_STATUSES: tuple[str, ...] = (
    "pending",
    "confirmed",
    "shipped",
    "delivered",
    "cancelled",
)

# Assumes hard deletes in Postgres (no is_deleted column).
# If soft deletes are added, add AND is_deleted = 0 here
# to match the ClickHouse query filter.
# Explicit UTC on created_at and NOW(): machine ran BST (UTC+1) during testing;
# without AT TIME ZONE 'UTC', Postgres used local time and diverged from ClickHouse
# (16 false missing orders on day 3 — see CONTEXT.md / Bug 4).
_POSTGRES_24H_COUNT_SQL = """
SELECT COUNT(*) FROM orders
WHERE created_at AT TIME ZONE 'UTC'
  >= NOW() AT TIME ZONE 'UTC' - INTERVAL '24 hours'
"""

# Explicit UTC via toTimeZone(created_at, 'UTC'): must match Postgres window above;
# same BST bug when ClickHouse compared naive/local wall-clock to UTC-stored values.
_CLICKHOUSE_24H_COUNT_SQL = """
SELECT count() FROM orders_current FINAL
WHERE is_deleted = 0
  AND toTimeZone(created_at, 'UTC') >= now() - INTERVAL 24 HOUR
"""


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


def connect_clickhouse() -> ClickHouseClient:
    # Port 9000 is the native TCP port inside Docker.
    # Port 19000 is only mapped to the host machine (see docker-compose.yml).
    # Airflow runs inside Docker so must use the internal port 9000.
    return ClickHouseClient(
        host=_env("CLICKHOUSE_HOST", "localhost"),
        port=int(_env("CLICKHOUSE_NATIVE_PORT", "9000")),
        user=_env("CLICKHOUSE_USER", "default"),
        password=_env("CLICKHOUSE_PASSWORD", ""),
        database=_env("CLICKHOUSE_DB", "default"),
    )


def check_orders_quality(client: ClickHouseClient | None = None) -> list[str]:
    """Validate orders_current (FINAL) for nulls, status domain, and positive amounts."""
    owns_client = client is None
    if owns_client:
        client = connect_clickhouse()
    failures: list[str] = []
    status_list = ", ".join(f"'{s}'" for s in ACCEPTED_ORDER_STATUSES)

    try:
        null_count = client.execute(
            """
            SELECT count()
            FROM orders_current FINAL
            WHERE isNull(order_id)
               OR isNull(customer_id)
               OR isNull(status)
               OR status = ''
            """
        )[0][0]
        if null_count:
            failures.append(
                f"orders_current: {null_count} row(s) with null/empty order_id, customer_id, or status"
            )

        invalid_status_count = client.execute(
            f"""
            SELECT count()
            FROM orders_current FINAL
            WHERE status NOT IN ({status_list})
            """
        )[0][0]
        if invalid_status_count:
            failures.append(
                f"orders_current: {invalid_status_count} row(s) with status outside "
                f"{ACCEPTED_ORDER_STATUSES}"
            )

        negative_amount_count = client.execute(
            """
            SELECT count()
            FROM orders_current FINAL
            WHERE total_amount < 0
            """
        )[0][0]
        if negative_amount_count:
            failures.append(
                f"orders_current: {negative_amount_count} row(s) with total_amount < 0"
            )
    finally:
        if owns_client:
            client.disconnect()

    return failures


def check_products_quality(client: ClickHouseClient | None = None) -> list[str]:
    """Validate products_current (FINAL) for non-negative inventory."""
    owns_client = client is None
    if owns_client:
        client = connect_clickhouse()
    failures: list[str] = []

    try:
        negative_inventory_count = client.execute(
            """
            SELECT count()
            FROM products_current FINAL
            WHERE inventory_count < 0
            """
        )[0][0]
        if negative_inventory_count:
            failures.append(
                f"products_current: {negative_inventory_count} row(s) with inventory_count < 0"
            )
    finally:
        if owns_client:
            client.disconnect()

    return failures


def check_end_to_end_integrity() -> tuple[bool, str]:
    """Compare 24-hour order counts between Postgres source and ClickHouse sink."""
    pg_conn = connect_postgres()
    ch_client = connect_clickhouse()
    try:
        with pg_conn.cursor() as cur:
            cur.execute(_POSTGRES_24H_COUNT_SQL)
            pg_row = cur.fetchone()
        if pg_row is None:
            raise RuntimeError("Postgres 24h count query returned no row")
        postgres_count = int(pg_row[0])

        ch_row = ch_client.execute(_CLICKHOUSE_24H_COUNT_SQL)
        if not ch_row:
            raise RuntimeError("ClickHouse 24h count query returned no row")
        clickhouse_count = int(ch_row[0][0])
    finally:
        pg_conn.close()
        ch_client.disconnect()

    if postgres_count == clickhouse_count:
        return True, f"PASS: {postgres_count} orders match"

    missing = postgres_count - clickhouse_count
    return (
        False,
        f"FAIL: Postgres={postgres_count} ClickHouse={clickhouse_count} Missing={missing}",
    )


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    load_dotenv()

    any_failed = False

    ch_client = connect_clickhouse()
    try:
        orders_failures = check_orders_quality(ch_client)
        if orders_failures:
            any_failed = True
            logger.warning("[orders quality] FAIL")
            for msg in orders_failures:
                logger.warning("  - %s", msg)
        else:
            logger.info("[orders quality] PASS")

        products_failures = check_products_quality(ch_client)
        if products_failures:
            any_failed = True
            logger.warning("[products quality] FAIL")
            for msg in products_failures:
                logger.warning("  - %s", msg)
        else:
            logger.info("[products quality] PASS")
    finally:
        ch_client.disconnect()

    e2e_ok, e2e_message = check_end_to_end_integrity()
    if e2e_ok:
        logger.info("[end-to-end integrity] %s", e2e_message)
    else:
        any_failed = True
        logger.warning("[end-to-end integrity] %s", e2e_message)

    if any_failed:
        logger.error("Checkpoint failed")
        sys.exit(1)

    logger.info("Checkpoint passed")
    sys.exit(0)


if __name__ == "__main__":
    main()
