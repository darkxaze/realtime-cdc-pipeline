"""
Schema Registry evolution tests for the CDC pipeline.

Validates that backward-compatible Avro changes are accepted and breaking
changes are rejected before they reach Flink or ClickHouse consumers.

Requires: docker compose up with schema-registry healthy.

Run: python tests/test_schema_evolution.py
"""

from __future__ import annotations

import json
import logging
import os
from typing import Any

import requests
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

PRODUCTS_VALUE_SUBJECT = "ecommerce.public.products-value"
SCHEMA_REGISTRY_HEADERS = {"Content-Type": "application/vnd.schemaregistry.v1+json"}
REQUEST_TIMEOUT_SEC = 10.0


def schema_registry_url() -> str:
    return os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081").rstrip("/")


def _products_schema(*, include_price: bool, include_weight_kg: bool) -> dict[str, Any]:
    """
    Flat products value schema — matches the subject format verified against
    Schema Registry (see curl example in project docs / test README).

    Rejected alternative: Debezium CDC envelope with nested before/after records —
    Avro rejected it as malformed (422) because the nested record was inlined twice.
    """
    fields: list[dict[str, Any]] = [
        {"name": "product_id", "type": "string"},
        {"name": "sku", "type": "string"},
        {"name": "name", "type": "string"},
        {"name": "category", "type": "string"},
    ]
    if include_price:
        fields.append({"name": "price", "type": "double"})
    fields.extend(
        [
            {"name": "inventory_count", "type": "int"},
            {"name": "updated_at", "type": "string"},
            {"name": "is_deleted", "type": "int"},
        ]
    )
    if include_weight_kg:
        fields.append({"name": "weight_kg", "type": ["null", "float"], "default": None})
    return {"type": "record", "name": "products", "fields": fields}


def _raise_for_status_with_body(response: requests.Response) -> None:
    if not response.ok:
        logger.error(
            "Schema Registry HTTP %s for %s: %s",
            response.status_code,
            response.url,
            response.text,
        )
    response.raise_for_status()


def ensure_subject_full_compatibility() -> None:
    """FULL rejects field removal; BACKWARD alone allows deletes (still 'compatible')."""
    url = f"{schema_registry_url()}/config/{PRODUCTS_VALUE_SUBJECT}"
    response = requests.put(
        url,
        headers=SCHEMA_REGISTRY_HEADERS,
        data=json.dumps({"compatibility": "FULL"}),
        timeout=REQUEST_TIMEOUT_SEC,
    )
    if response.status_code not in {200, 201}:
        _raise_for_status_with_body(response)
    logger.info("Subject %s compatibility set to FULL", PRODUCTS_VALUE_SUBJECT)


def reset_subject() -> None:
    """Delete subject so reruns exercise compatibility checks, not duplicate-schema 200s."""
    url = f"{schema_registry_url()}/subjects/{PRODUCTS_VALUE_SUBJECT}"
    response = requests.delete(url, timeout=REQUEST_TIMEOUT_SEC)
    if response.status_code in {200, 204, 404}:
        logger.info("Subject %s reset (HTTP %s)", PRODUCTS_VALUE_SUBJECT, response.status_code)
        return
    _raise_for_status_with_body(response)


def register_schema(schema_dict: dict[str, Any]) -> requests.Response:
    url = f"{schema_registry_url()}/subjects/{PRODUCTS_VALUE_SUBJECT}/versions"
    payload = {"schemaType": "AVRO", "schema": json.dumps(schema_dict)}
    # data= preserves Content-Type; json= would override with application/json (422).
    return requests.post(
        url,
        headers=SCHEMA_REGISTRY_HEADERS,
        data=json.dumps(payload),
        timeout=REQUEST_TIMEOUT_SEC,
    )


def ensure_base_schema_registered() -> None:
    """
    Seed the subject with a baseline schema (no weight_kg).

    Idempotent across reruns: identical schema re-post returns 409, which is fine.
    """
    response = register_schema(_products_schema(include_price=True, include_weight_kg=False))
    if response.status_code in {200, 201}:
        logger.info("Registered baseline schema for subject=%s", PRODUCTS_VALUE_SUBJECT)
        return
    if response.status_code == 409:
        logger.info("Baseline schema already registered for subject=%s", PRODUCTS_VALUE_SUBJECT)
        return
    _raise_for_status_with_body(response)


def test_backward_compatible_change() -> bool:
    """Register optional weight_kg — must be accepted as a backward-compatible evolution."""
    ensure_subject_full_compatibility()
    ensure_base_schema_registered()

    response = register_schema(_products_schema(include_price=True, include_weight_kg=True))
    if response.status_code not in {200, 201}:
        logger.error(
            "Expected HTTP 200/201 for backward-compatible schema; got %s body=%s",
            response.status_code,
            response.text,
        )
        print("FAIL — backward compatible change rejected")
        return False

    print("PASS — backward compatible change accepted")
    return True


def test_breaking_change_rejected() -> bool:
    """Removing price violates FULL compatibility — Registry must reject before production."""
    ensure_subject_full_compatibility()
    ensure_base_schema_registered()
    compat_response = register_schema(_products_schema(include_price=True, include_weight_kg=True))
    if compat_response.status_code not in {200, 201}:
        logger.error(
            "Failed to seed schema with price before breaking test: %s body=%s",
            compat_response.status_code,
            compat_response.text,
        )
        print("FAIL — could not seed schema for breaking change test")
        return False

    response = register_schema(_products_schema(include_price=False, include_weight_kg=True))
    if response.status_code not in {409, 422}:
        logger.error(
            "Expected HTTP 409/422 for breaking schema; got %s body=%s",
            response.status_code,
            response.text,
        )
        print("FAIL — breaking change not rejected")
        return False

    if "compatibility" not in response.text.lower():
        logger.error("Expected 'compatibility' in rejection body: %s", response.text)
        print("FAIL — breaking change rejected without compatibility detail")
        return False

    print("PASS — breaking change correctly rejected")
    return True


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )


def main() -> None:
    load_dotenv()
    configure_logging()
    reset_subject()

    results: list[tuple[str, bool]] = [
        ("backward compatible change", test_backward_compatible_change()),
        ("breaking change rejected", test_breaking_change_rejected()),
    ]

    passed = sum(1 for _, ok in results if ok)
    total = len(results)

    print("")
    print("Summary")
    for name, ok in results:
        print(f"  {name}: {'PASS' if ok else 'FAIL'}")
    print(f"  overall: {passed}/{total} passed")

    if passed != total:
        raise SystemExit(1)


if __name__ == "__main__":
    try:
        main()
    except requests.RequestException as exc:
        logger.exception("Schema Registry request failed: %s", exc)
        print("FAIL — could not reach Schema Registry")
        raise SystemExit(1) from exc
