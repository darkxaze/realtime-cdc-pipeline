"""
Flash sale analytics report from the dbt gold mart.

These numbers come from mart_flash_sale_analysis, which reads live ClickHouse
tables populated by the real pipeline. They are not fabricated.
"""

from __future__ import annotations

import logging
import os
import sys
from dataclasses import dataclass

from clickhouse_driver import Client as ClickHouseClient
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

_MART_QUERY = """
SELECT
    flash_sale_avg_confirm_seconds,
    normal_avg_confirm_seconds,
    slowdown_factor,
    flash_sale_revenue,
    normal_revenue,
    revenue_multiplier,
    normal_cancel_rate,
    flash_sale_cancel_rate,
    products_sold_out
FROM gold.mart_flash_sale_analysis
LIMIT 1
"""


@dataclass(frozen=True)
class FlashSaleMetrics:
    flash_sale_avg_confirm_seconds: float
    normal_avg_confirm_seconds: float
    slowdown_factor: float
    flash_sale_revenue: float
    normal_revenue: float
    revenue_multiplier: float
    normal_cancel_rate: float
    flash_sale_cancel_rate: float
    products_sold_out: int


def _env(name: str, default: str) -> str:
    return os.getenv(name, default)


def connect_clickhouse() -> ClickHouseClient:
    return ClickHouseClient(
        host=_env("CLICKHOUSE_HOST", "localhost"),
        port=int(_env("CLICKHOUSE_NATIVE_PORT", "19000")),
        user=_env("CLICKHOUSE_USER", "default"),
        password=_env("CLICKHOUSE_PASSWORD", ""),
        database=_env("CLICKHOUSE_DB", "default"),
    )


def fetch_flash_sale_metrics(client: ClickHouseClient) -> FlashSaleMetrics:
    rows = client.execute(_MART_QUERY)
    if not rows:
        raise RuntimeError(
            "gold.mart_flash_sale_analysis is empty; run dbt run --select mart_flash_sale_analysis "
            "after flash sale load generator data exists"
        )

    row = rows[0]
    return FlashSaleMetrics(
        flash_sale_avg_confirm_seconds=float(row[0]),
        normal_avg_confirm_seconds=float(row[1]),
        slowdown_factor=float(row[2]),
        flash_sale_revenue=float(row[3]),
        normal_revenue=float(row[4]),
        revenue_multiplier=float(row[5]),
        normal_cancel_rate=float(row[6]),
        flash_sale_cancel_rate=float(row[7]),
        products_sold_out=int(row[8]),
    )


def print_report(metrics: FlashSaleMetrics) -> None:
    normal_avg = metrics.normal_avg_confirm_seconds
    flash_avg = metrics.flash_sale_avg_confirm_seconds
    slowdown = metrics.slowdown_factor
    flash_revenue = metrics.flash_sale_revenue
    normal_revenue = metrics.normal_revenue
    revenue_mult = metrics.revenue_multiplier
    normal_cancel = metrics.normal_cancel_rate
    flash_cancel = metrics.flash_sale_cancel_rate
    sold_out = metrics.products_sold_out

    logger.info("========================================")
    logger.info("  Flash Sale Analytics")
    logger.info("  CDC Pipeline — Measured Results")
    logger.info("========================================")
    logger.info("")
    logger.info("Order Confirmation Latency")
    logger.info(f"  Normal traffic:        {normal_avg:.1f} seconds")
    logger.info(f"  Flash sale:            {flash_avg:.1f} seconds")
    logger.info(f"  Slowdown factor:       {slowdown:.1f}x slower")
    logger.info("")
    logger.info(f"  Finding: Order confirmation is {slowdown:.1f}x slower during")
    logger.info("  flash sale — a bottleneck invisible to batch analytics.")
    logger.info("  This metric exists because CDC captures every status transition.")
    logger.info("")
    logger.info("Revenue")
    logger.info(f"  Flash sale window:    £{flash_revenue:,.2f}")
    logger.info(f"  Normal same duration: £{normal_revenue:,.2f}")
    logger.info(f"  Multiplier:           {revenue_mult:.1f}x")
    logger.info("")
    logger.info("Cancellations")
    logger.info(f"  Normal rate:          {normal_cancel:.1%}")
    logger.info(f"  Flash sale rate:      {flash_cancel:.1%}")
    logger.info("")
    logger.info(f"Products sold out:      {sold_out}")
    logger.info("")
    logger.info("========================================")
    logger.info("Reproduce: python analysis/flash_sale_analysis.py")
    logger.info("========================================")


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    load_dotenv()

    client = connect_clickhouse()
    try:
        metrics = fetch_flash_sale_metrics(client)
    finally:
        client.disconnect()

    print_report(metrics)
    logger.info("Flash sale report generated from gold.mart_flash_sale_analysis")


if __name__ == "__main__":
    try:
        main()
    except RuntimeError as exc:
        logger.error("%s", exc)
        sys.exit(1)
