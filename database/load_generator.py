"""
Postgres e-commerce load generator for Stage 1 (seed + sustained load).

flash_sale: configurable via LOAD_FLASH_DURATION_SEC and LOAD_FLASH_TPS (defaults 300s, 50 TPS);
workload mix 80% new orders / 15% status / 5% order delete + inventory restore (CDC tombstones).
"""

from __future__ import annotations

import argparse
import logging
import os
import random
import threading
import time
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from contextlib import contextmanager
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Generator, Literal, Sequence
from uuid import UUID, uuid4

import psycopg2
from dotenv import load_dotenv
from faker import Faker
from psycopg2.extensions import connection
from psycopg2.pool import ThreadedConnectionPool
from psycopg2.extras import execute_batch

logger = logging.getLogger(__name__)

CATEGORIES: tuple[str, ...] = (
    "electronics",
    "apparel",
    "home",
    "beauty",
    "sports",
    "books",
    "toys",
    "grocery",
)

STATUS_CHAIN: dict[str, str] = {
    "pending": "confirmed",
    "confirmed": "shipped",
    "shipped": "delivered",
}


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    return int(raw)


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    return float(raw)


def _seeded_rng() -> random.Random | None:
    """Deterministic RNG for unique suffixes when LOAD_SEED is set (uuid4() ignores random.seed)."""
    raw = os.getenv("LOAD_SEED")
    if raw is None or raw.strip() == "":
        return None
    return random.Random(int(raw))


def _unique_suffix(rng: random.Random | None) -> str:
    if rng is not None:
        return format(rng.getrandbits(32), "08x")
    return uuid4().hex[:8]


def _sku_body(rng: random.Random | None) -> str:
    if rng is not None:
        return format(rng.getrandbits(40), "010x")
    return uuid4().hex[:10]


@dataclass
class LoadStats:
    """Counters for timed load modes (protected by lock)."""

    success: int = 0
    errors: int = 0
    _lock: threading.Lock = field(default_factory=threading.Lock)

    def record_success(self) -> None:
        with self._lock:
            self.success += 1

    def record_error(self) -> None:
        with self._lock:
            self.errors += 1


def _get_env(name: str) -> str:
    if name not in os.environ:
        raise KeyError(f"Missing required environment variable: {name}")
    return os.environ[name]


def create_pool(
    minconn: int | None = None,
    maxconn: int | None = None,
) -> ThreadedConnectionPool:
    """
    Build a pool from POSTGRES_* vars.

    Pool size can be tuned with POSTGRES_POOL_MINCONN / POSTGRES_POOL_MAXCONN (defaults 2 / 96).

    Rejected alternative: single connection — would serialize load and hide
    realistic lock contention on inventory and orders under higher TPS.
    """
    mc = _env_int("POSTGRES_POOL_MINCONN", 2) if minconn is None else minconn
    xc = _env_int("POSTGRES_POOL_MAXCONN", 96) if maxconn is None else maxconn
    if mc < 1 or xc < 1 or mc > xc:
        raise ValueError(
            f"Invalid pool bounds: minconn={mc}, maxconn={xc} "
            "(check POSTGRES_POOL_MINCONN / POSTGRES_POOL_MAXCONN)."
        )
    return ThreadedConnectionPool(
        minconn=mc,
        maxconn=xc,
        host=_get_env("POSTGRES_HOST"),
        port=int(_get_env("POSTGRES_PORT")),
        user=_get_env("POSTGRES_USER"),
        password=_get_env("POSTGRES_PASSWORD"),
        dbname=_get_env("POSTGRES_DB"),
    )


@contextmanager
def pooled_connection(pool: ThreadedConnectionPool) -> Generator[connection, None, None]:
    conn = pool.getconn()
    try:
        yield conn
    finally:
        pool.putconn(conn)


def weighted_customer_tier() -> str:
    """70% standard, 20% premium, 10% vip."""
    return random.choices(
        ("standard", "premium", "vip"),
        weights=(70, 20, 10),
        k=1,
    )[0]


def pick_workload_mix() -> Literal["new_order", "status_update", "tier_update"]:
    """60% new orders, 30% status transitions, 10% tier updates."""
    r = random.random()
    if r < 0.60:
        return "new_order"
    if r < 0.90:
        return "status_update"
    return "tier_update"


def pick_flash_workload_mix() -> Literal["new_order", "flash_status_update", "cancel_order"]:
    """80% new orders, 15% status (pending/confirmed lane), 5% cancellations."""
    r = random.random()
    if r < 0.80:
        return "new_order"
    if r < 0.95:
        return "flash_status_update"
    return "cancel_order"


def _random_money(low: float, high: float) -> Decimal:
    return Decimal(str(round(random.uniform(low, high), 2)))


def seed_customers_and_products(pool: ThreadedConnectionPool) -> tuple[int, int, float]:
    """Bulk insert customers and products; counts and inventory range from env."""
    n_customers = _env_int("SEED_CUSTOMER_COUNT", 1000)
    n_products = _env_int("SEED_PRODUCT_COUNT", 200)
    # Low starting inventory makes flash sale products sell out during the 300 second
    # window, which is the behaviour mart_flash_sale_analysis and the Grafana inventory
    # panel are designed to show.
    inv_min = _env_int("SEED_INVENTORY_MIN", 50)
    inv_max = _env_int("SEED_INVENTORY_MAX", 200)
    if inv_min < 0 or inv_max < inv_min:
        raise ValueError(
            f"Invalid inventory bounds: SEED_INVENTORY_MIN={inv_min}, SEED_INVENTORY_MAX={inv_max}"
        )

    fake = Faker()
    seed = os.getenv("LOAD_SEED")
    rng = _seeded_rng()
    if seed is not None:
        fake.seed_instance(int(seed))
        random.seed(int(seed))

    t0 = time.perf_counter()
    customer_rows: list[tuple[str, str]] = []
    for i in range(n_customers):
        email = f"{fake.user_name()}_{i}_{_unique_suffix(rng)}@example.com"
        customer_rows.append((email, weighted_customer_tier()))

    product_rows: list[tuple[str, str, str, Decimal, int]] = []
    for i in range(n_products):
        sku = f"SKU-{_sku_body(rng)}-{i}"[:50]
        name = fake.catch_phrase()[:255]
        category = random.choice(CATEGORIES)
        price = _random_money(5.99, 299.99)
        inv = random.randint(inv_min, inv_max)
        product_rows.append((sku, name, category, price, inv))

    with pooled_connection(pool) as conn:
        conn.autocommit = False
        try:
            with conn.cursor() as cur:
                execute_batch(
                    cur,
                    "INSERT INTO customers (email, tier) VALUES (%s, %s)",
                    customer_rows,
                    page_size=500,
                )
                execute_batch(
                    cur,
                    (
                        "INSERT INTO products (sku, name, category, price, inventory_count) "
                        "VALUES (%s, %s, %s, %s, %s)"
                    ),
                    product_rows,
                    page_size=200,
                )
            conn.commit()
        except psycopg2.Error:
            conn.rollback()
            raise

    elapsed = time.perf_counter() - t0
    return n_customers, n_products, elapsed


def _fetch_one_uuid(cur: psycopg2.extensions.cursor, query: str, params: tuple) -> UUID | None:
    cur.execute(query, params)
    row = cur.fetchone()
    if row is None:
        return None
    return UUID(str(row[0]))


def _fetch_product_line_from_candidates(
    cur: psycopg2.extensions.cursor,
    candidate_ids: Sequence[UUID],
) -> list[tuple[UUID, Decimal, int]]:
    """1–3 order lines drawn only from candidate product_ids that still have stock."""
    if not candidate_ids:
        raise ValueError("Flash sale requires non-empty candidate product ids.")
    id_tuple = tuple(str(pid) for pid in candidate_ids)
    cur.execute(
        (
            "SELECT product_id, price, inventory_count FROM products "
            "WHERE product_id IN %s AND inventory_count > 0"
        ),
        (id_tuple,),
    )
    rows = list(cur.fetchall())
    if not rows:
        raise ValueError("No in-stock products among flash candidates.")
    random.shuffle(rows)
    take = min(random.randint(1, 3), len(rows))
    lines: list[tuple[UUID, Decimal, int]] = []
    for r in rows[:take]:
        pid, price, inv = UUID(str(r[0])), Decimal(str(r[1])), int(r[2])
        qty = min(random.randint(1, 3), max(1, inv))
        lines.append((pid, price, qty))
    return lines


def _fetch_product_line(
    cur: psycopg2.extensions.cursor,
) -> list[tuple[UUID, Decimal, int]]:
    """1–3 order lines: (product_id, unit_price, qty) with stock."""
    n_lines = random.randint(1, 3)
    cur.execute(
        (
            "SELECT product_id, price, inventory_count FROM products "
            "WHERE inventory_count > 0 ORDER BY RANDOM() LIMIT %s"
        ),
        (n_lines,),
    )
    rows = cur.fetchall()
    lines: list[tuple[UUID, Decimal, int]] = []
    for r in rows:
        pid, price, inv = UUID(str(r[0])), Decimal(str(r[1])), int(r[2])
        qty = min(random.randint(1, 3), max(1, inv))
        lines.append((pid, price, qty))
    return lines


def transaction_new_order(
    conn: connection,
    flash_product_ids: Sequence[UUID] | None = None,
) -> None:
    with conn.cursor() as cur:
        cid = _fetch_one_uuid(cur, "SELECT customer_id FROM customers ORDER BY RANDOM() LIMIT 1", ())
        if cid is None:
            raise ValueError("No customers available; run seed first.")

        if flash_product_ids is not None:
            lines = _fetch_product_line_from_candidates(cur, flash_product_ids)
        else:
            lines = _fetch_product_line(cur)
        if not lines:
            raise ValueError("No in-stock products available; run seed first.")

        total = sum((unit * qty for _, unit, qty in lines), start=Decimal("0.00"))
        if total <= 0:
            raise ValueError("Computed non-positive order total.")

        cur.execute(
            (
                "INSERT INTO orders (customer_id, status, total_amount) "
                "VALUES (%s, 'pending', %s) RETURNING order_id"
            ),
            (str(cid), str(total)),
        )
        oid_row = cur.fetchone()
        if oid_row is None:
            raise RuntimeError("INSERT order did not return order_id.")
        order_id = UUID(str(oid_row[0]))

        for product_id, unit_price, qty in lines:
            cur.execute(
                (
                    "INSERT INTO order_items (order_id, product_id, quantity, unit_price) "
                    "VALUES (%s, %s, %s, %s)"
                ),
                (str(order_id), str(product_id), qty, str(unit_price)),
            )
            cur.execute(
                (
                    "UPDATE products SET inventory_count = inventory_count - %s "
                    "WHERE product_id = %s AND inventory_count >= %s"
                ),
                (qty, str(product_id), qty),
            )
            if cur.rowcount != 1:
                # At 50 TPS with 20 hot products and multiple threads, inventory
                # contention is expected. The guard WHERE inventory_count >= quantity
                # prevents overselling. A rowcount of 0 means another thread won the
                # race — skipping is correct.
                # Another thread claimed this inventory first — skip silently.
                # This is expected under concurrent flash sale load and should
                # not count as an error.
                conn.rollback()
                return


def transaction_status_update(conn: connection) -> None:
    with conn.cursor() as cur:
        cur.execute(
            (
                "SELECT order_id, status FROM orders "
                "WHERE status IN ('pending','confirmed','shipped') "
                "ORDER BY RANDOM() LIMIT 1"
            )
        )
        row = cur.fetchone()
        if row is None:
            raise ValueError("No orders eligible for status update.")
        order_id, status = UUID(str(row[0])), str(row[1])
        nxt = STATUS_CHAIN.get(status)
        if nxt is None:
            raise ValueError(f"Unexpected order status: {status}")
        cur.execute(
            "UPDATE orders SET status = %s WHERE order_id = %s",
            (nxt, str(order_id)),
        )


def transaction_flash_status_update(conn: connection) -> None:
    """Advance only pending→confirmed→shipped (no shipped→delivered in flash mix)."""
    with conn.cursor() as cur:
        cur.execute(
            (
                "SELECT order_id, status FROM orders "
                "WHERE status IN ('pending','confirmed') "
                "ORDER BY RANDOM() LIMIT 1"
            )
        )
        row = cur.fetchone()
        if row is None:
            raise ValueError("No orders eligible for flash status update.")
        order_id, status = UUID(str(row[0])), str(row[1])
        nxt = STATUS_CHAIN.get(status)
        if nxt is None:
            raise ValueError(f"Unexpected order status: {status}")
        cur.execute(
            "UPDATE orders SET status = %s WHERE order_id = %s",
            (nxt, str(order_id)),
        )


def transaction_order_cancellation(conn: connection) -> None:
    # Tombstone path: DELETE emits remove events so sinks can reconcile deletes (vs silent orphans).
    with conn.cursor() as cur:
        cur.execute(
            "SELECT order_id FROM orders "
            "WHERE status IN ('pending', 'confirmed') "
            "ORDER BY RANDOM() LIMIT 1",
        )
        row = cur.fetchone()
        if row is None:
            raise ValueError("No orders eligible for cancellation.")
        order_id = UUID(str(row[0]))

        cur.execute(
            "SELECT product_id, quantity FROM order_items WHERE order_id = %s",
            (str(order_id),),
        )
        items = cur.fetchall()

        for r in items:
            pid, qty = UUID(str(r[0])), int(r[1])
            cur.execute(
                (
                    "UPDATE products SET inventory_count = inventory_count + %s "
                    "WHERE product_id = %s"
                ),
                (qty, str(pid)),
            )
            if cur.rowcount != 1:
                raise RuntimeError("Inventory restore did not affect exactly one product row.")

        cur.execute("DELETE FROM order_items WHERE order_id = %s", (str(order_id),))
        cur.execute("DELETE FROM orders WHERE order_id = %s", (str(order_id),))
        if cur.rowcount != 1:
            raise RuntimeError("DELETE orders did not affect exactly one row.")


def transaction_tier_update(conn: connection) -> None:
    new_tier = random.choice(("standard", "premium", "vip"))
    with conn.cursor() as cur:
        cid = _fetch_one_uuid(cur, "SELECT customer_id FROM customers ORDER BY RANDOM() LIMIT 1", ())
        if cid is None:
            raise ValueError("No customers available.")
        cur.execute(
            "UPDATE customers SET tier = %s WHERE customer_id = %s",
            (new_tier, str(cid)),
        )


def run_single_timed_transaction(
    pool: ThreadedConnectionPool,
    kind: str,
    flash_product_ids: Sequence[UUID] | None = None,
) -> None:
    with pooled_connection(pool) as conn:
        conn.autocommit = False
        try:
            if kind == "new_order":
                transaction_new_order(conn, flash_product_ids)
            elif kind == "status_update":
                transaction_status_update(conn)
            elif kind == "flash_status_update":
                transaction_flash_status_update(conn)
            elif kind == "cancel_order":
                transaction_order_cancellation(conn)
            else:
                transaction_tier_update(conn)
            conn.commit()
        except (psycopg2.Error, ValueError, RuntimeError):
            conn.rollback()
            raise


def dispatch_timed_transaction(
    pool: ThreadedConnectionPool,
    flash_sale: bool = False,
    flash_product_ids: Sequence[UUID] | None = None,
) -> None:
    """Pick workload mix; one transaction per scheduled tick."""
    kind = pick_flash_workload_mix() if flash_sale else pick_workload_mix()
    ids = flash_product_ids if flash_sale else None
    run_single_timed_transaction(pool, kind, ids)


def run_timed_load(
    pool: ThreadedConnectionPool,
    duration_seconds: float,
    target_tps: float,
    max_pool_connections: int,
    flash_sale: bool = False,
    flash_product_ids: Sequence[UUID] | None = None,
) -> LoadStats:
    """
    Submit work at target_tps using a thread pool; measure successes vs errors.

    max_pool_connections caps worker threads so we do not exhaust ThreadedConnectionPool
    (e.g. 128 workers vs 96 pool slots).

    Rejected alternative: one thread sleeping 1/target_tps — would not expose
    concurrent inventory contention that CDC benchmarks care about.
    """
    stats = LoadStats()
    stop_at = time.perf_counter() + duration_seconds
    interval = 1.0 / target_tps
    next_emit = time.perf_counter()
    max_workers = min(
        max_pool_connections,
        min(128, max(32, int(target_tps * 4))),
    )
    max_inflight = min(
        max_pool_connections * 3,
        max(max_workers * 4, int(target_tps * 4)),
    )
    executor = ThreadPoolExecutor(max_workers=max_workers)
    pending: set[Future[None]] = set()

    def consume_future(fut: Future[None]) -> None:
        exc = fut.exception()
        if exc is None:
            stats.record_success()
        else:
            stats.record_error()
            logger.debug("Transaction failed", exc_info=(type(exc), exc, exc.__traceback__))

    try:
        while time.perf_counter() < stop_at:
            while len(pending) >= max_inflight:
                done, pending = wait(pending, return_when=FIRST_COMPLETED)
                for fut in done:
                    consume_future(fut)

            now = time.perf_counter()
            sleep_for = next_emit - now
            if sleep_for > 0:
                time.sleep(sleep_for)
            next_emit += interval

            pending.add(
                executor.submit(
                    dispatch_timed_transaction,
                    pool,
                    flash_sale,
                    flash_product_ids,
                )
            )

        while pending:
            done, pending = wait(pending, return_when=FIRST_COMPLETED)
            for fut in done:
                consume_future(fut)
    finally:
        executor.shutdown(wait=True, cancel_futures=False)

    return stats


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )


def cmd_seed(pool: ThreadedConnectionPool) -> None:
    n_cust, n_prod, elapsed = seed_customers_and_products(pool)
    logger.info(
        "Inserted %s customers, %s products, time taken %.2fs",
        n_cust,
        n_prod,
        elapsed,
    )


def cmd_normal(pool: ThreadedConnectionPool, duration: int) -> None:
    target_tps = _env_float("LOAD_NORMAL_TPS", 10.0)
    stats = run_timed_load(
        pool,
        float(duration),
        target_tps=target_tps,
        max_pool_connections=pool.maxconn,
    )
    duration_f = float(duration)
    achieved = stats.success / duration_f if duration_f > 0 else 0.0
    logger.info(
        "target_tps=%s achieved_tps=%.4f total_transactions=%s error_count=%s duration_seconds=%s",
        target_tps,
        achieved,
        stats.success,
        stats.errors,
        duration,
    )


def cmd_flash_sale(pool: ThreadedConnectionPool) -> None:
    duration_seconds = _env_float("LOAD_FLASH_DURATION_SEC", 300.0)
    target_tps = _env_float("LOAD_FLASH_TPS", 50.0)
    # Flash sale targets low-inventory products to stress-test the inventory decrement logic and create sold-out conditions for dashboard testing.
    rows: list[tuple] = []
    with pooled_connection(pool) as conn:
        conn.autocommit = False
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT product_id FROM products ORDER BY inventory_count ASC LIMIT 20",
                )
                rows = cur.fetchall()
            conn.rollback()
        except psycopg2.Error:
            conn.rollback()
            raise
    if not rows:
        raise ValueError("No products in database; run seed before flash_sale.")
    flash_candidates: tuple[UUID, ...] = tuple(UUID(str(r[0])) for r in rows)
    stats = run_timed_load(
        pool,
        duration_seconds,
        target_tps=target_tps,
        max_pool_connections=pool.maxconn,
        flash_sale=True,
        flash_product_ids=flash_candidates,
    )
    achieved = stats.success / duration_seconds if duration_seconds > 0 else 0.0
    logger.info(
        "target_tps=%s achieved_tps=%.4f total_transactions=%s error_count=%s duration_seconds=%s",
        target_tps,
        achieved,
        stats.success,
        stats.errors,
        int(duration_seconds),
    )


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="E-commerce Postgres load generator (seed / normal / flash_sale).",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("seed", help="Insert customers and products (counts from env).")

    p_normal = sub.add_parser(
        "normal",
        help="Mixed workload; TPS from LOAD_NORMAL_TPS, duration from --duration.",
    )
    p_normal.add_argument(
        "--duration",
        type=int,
        default=60,
        help="Run length in seconds (default: 60).",
    )

    sub.add_parser(
        "flash_sale",
        help="High-TPS mix (80/15/5); TPS and duration from LOAD_FLASH_* env vars.",
    )
    return parser


def main() -> None:
    load_dotenv()
    configure_logging()
    parser = build_arg_parser()
    args = parser.parse_args()

    pool: ThreadedConnectionPool | None = None
    try:
        pool = create_pool()
        if args.command == "seed":
            cmd_seed(pool)
        elif args.command == "normal":
            cmd_normal(pool, args.duration)
        elif args.command == "flash_sale":
            cmd_flash_sale(pool)
        else:
            raise ValueError(f"Unknown command: {args.command}")
    except KeyboardInterrupt:
        logger.info("Interrupted by user.")
        raise SystemExit(130) from None
    except psycopg2.Error as exc:
        logger.exception("Database error: %s", exc)
        raise SystemExit(1) from exc
    except (OSError, KeyError, ValueError) as exc:
        logger.exception("Configuration or validation error: %s", exc)
        raise SystemExit(1) from exc
    finally:
        if pool is not None:
            pool.closeall()


if __name__ == "__main__":
    main()
