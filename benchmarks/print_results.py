"""
Print a formatted summary of measured CDC pipeline benchmark results.

Reads JSON output files produced by benchmarks/latency_test.py and
benchmarks/e2e_latency_test.py.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

logger = logging.getLogger(__name__)

BENCHMARKS_DIR = Path(__file__).resolve().parent

STAGE2_PATH = BENCHMARKS_DIR / "stage2_latency.json"
STAGE3_NORMAL_PATH = BENCHMARKS_DIR / "stage3_e2e_latency_normal.json"
STAGE3_FLASH_PATH = BENCHMARKS_DIR / "stage3_e2e_latency_flash_sale.json"


def load_benchmark(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        logger.warning("Missing benchmark file: %s — skipping", path)
        return None

    try:
        with path.open(encoding="utf-8") as handle:
            data = json.load(handle)
    except (json.JSONDecodeError, OSError, UnicodeDecodeError) as exc:
        logger.warning("Could not read benchmark file %s: %s — skipping", path, exc)
        return None

    if not isinstance(data, dict):
        logger.warning("Unexpected JSON shape in %s — skipping", path)
        return None

    return data


def _format_ms(value: Any) -> str:
    if value is None:
        return "n/a"
    return f"{float(value):.0f}ms"


def _format_stage2_line(data: dict[str, Any]) -> str:
    return (
        f"    p50: {_format_ms(data.get('p50_ms'))}  "
        f"p95: {_format_ms(data.get('p95_ms'))}  "
        f"p99: {_format_ms(data.get('p99_ms'))}  "
        f"samples: {data.get('sample_count', 'n/a')}"
    )


def _format_stage3_line(data: dict[str, Any]) -> str:
    return (
        f"    p50: {_format_ms(data.get('p50_ms'))}  "
        f"p95: {_format_ms(data.get('p95_ms'))}  "
        f"p99: {_format_ms(data.get('p99_ms'))}  "
        f"samples: {data.get('sample_count', 'n/a')}  "
        f"timeouts: {data.get('timeout_count', 'n/a')}"
    )


def print_summary() -> None:
    stage2 = load_benchmark(STAGE2_PATH)
    stage3_normal = load_benchmark(STAGE3_NORMAL_PATH)
    stage3_flash = load_benchmark(STAGE3_FLASH_PATH)

    logger.info("========================================")
    logger.info("  CDC Pipeline — Benchmark Results")
    logger.info("  Reproducible: docker-compose up then")
    logger.info("  python benchmarks/print_results.py")
    logger.info("========================================")
    logger.info("")

    if stage2 is not None:
        logger.info("  Postgres to Kafka (stage 2):")
        logger.info(_format_stage2_line(stage2))
        logger.info("")

    if stage3_normal is not None:
        logger.info("  Postgres to ClickHouse normal 10 TPS (stage 3):")
        logger.info(_format_stage3_line(stage3_normal))
        logger.info("")

    if stage3_flash is not None:
        logger.info("  Postgres to ClickHouse flash sale 50 TPS (stage 3):")
        logger.info(_format_stage3_line(stage3_flash))
        logger.info("")


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    load_dotenv()
    print_summary()


if __name__ == "__main__":
    main()
