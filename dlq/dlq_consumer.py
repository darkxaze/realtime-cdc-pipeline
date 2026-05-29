"""
Dead-letter queue consumer and replay tool for the CDC pipeline.

DLQ architecture:
  Skipping bad events loses auditability and makes row-level reconciliation impossible
  after schema drift or sink failures. Blocking the entire pipeline on one poison record
  stalls all healthy tables and breaks SLA during flash sale load.

  DLQ isolates failures: persist coordinates + raw payload, alert operators on sink
  failures, replay when fixed. Validation failures are logged without paging; sink failures
  page Slack with a replay command. Deserialisation failures are log-only — Schema Registry
  is the production guard; if they appear, Registry or converter config is broken.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Final

import requests
from clickhouse_driver import Client as ClickHouseClient
from clickhouse_driver.errors import Error as ClickHouseError
from confluent_kafka import Consumer, KafkaError, KafkaException, Producer
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

DLQ_TOPIC: Final[str] = os.getenv("KAFKA_DLQ_TOPIC", "dead_letter_queue")
CONSUMER_GROUP: Final[str] = os.getenv("KAFKA_DLQ_CONSUMER_GROUP", "dlq-consumer")

FAILURE_SINK: Final[str] = "sink_failure"
FAILURE_VALIDATION: Final[str] = "validation_failure"
FAILURE_DESERIALISATION: Final[str] = "deserialisation_failure"

# ClickHouse client / execute failures during DLQ persistence or replay bookkeeping.
_CLICKHOUSE_WRITE_ERRORS: Final[tuple[type[BaseException], ...]] = (ClickHouseError, OSError)

# Handler routing failures not covered by malformed-message parsing.
_CONSUME_HANDLER_ERRORS: Final[tuple[type[BaseException], ...]] = (
    ClickHouseError,
    OSError,
    requests.RequestException,
    KafkaException,
    BufferError,
)

# Kafka produce/flush or ClickHouse UPDATE after successful republish.
_REPLAY_ERRORS: Final[tuple[type[BaseException], ...]] = (
    ClickHouseError,
    OSError,
    KafkaException,
    BufferError,
    UnicodeEncodeError,
)


@dataclass(frozen=True)
class DLQEvent:
    """Row shape for ClickHouse dlq_events (replayed_at set only after successful replay)."""

    original_topic: str
    original_offset: int
    original_timestamp: datetime
    failure_reason: str
    failure_detail: str
    raw_payload: str
    failed_at: datetime
    replayed_at: datetime | None = None


def _env(name: str, default: str) -> str:
    return os.getenv(name, default)


def _kafka_bootstrap() -> str:
    return _env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")


def _slack_request_timeout_sec() -> float:
    return float(_env("SLACK_REQUEST_TIMEOUT_SEC", "10"))


def _kafka_producer_flush_timeout_sec() -> float:
    return float(_env("KAFKA_PRODUCER_FLUSH_TIMEOUT_SEC", "30"))


def connect_clickhouse() -> ClickHouseClient:
    return ClickHouseClient(
        host=_env("CLICKHOUSE_HOST", "localhost"),
        port=int(_env("CLICKHOUSE_NATIVE_PORT", "19000")),
        user=_env("CLICKHOUSE_USER", "default"),
        password=_env("CLICKHOUSE_PASSWORD", ""),
        database=_env("CLICKHOUSE_DB", "default"),
    )


def _parse_timestamp(value: Any) -> datetime:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    if isinstance(value, (int, float)):
        if value > 1_000_000_000_000:
            return datetime.fromtimestamp(value / 1000.0, tz=timezone.utc)
        return datetime.fromtimestamp(value, tz=timezone.utc)
    if isinstance(value, str):
        normalized = value.replace("Z", "+00:00")
        try:
            parsed = datetime.fromisoformat(normalized)
        except ValueError as exc:
            raise ValueError(f"Unparseable timestamp: {value!r}") from exc
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    raise ValueError(f"Unsupported timestamp type: {type(value)!r}")


def _parse_dlq_message(payload: bytes) -> DLQEvent:
    data = json.loads(payload.decode("utf-8"))
    return DLQEvent(
        original_topic=str(data["original_topic"]),
        original_offset=int(data["original_offset"]),
        original_timestamp=_parse_timestamp(data["original_timestamp"]),
        failure_reason=str(data["failure_reason"]),
        failure_detail=str(data.get("failure_detail", "")),
        raw_payload=str(data.get("raw_payload", "")),
        failed_at=_parse_timestamp(data.get("failed_at", datetime.now(timezone.utc))),
    )


def _insert_dlq_event(client: ClickHouseClient, event: DLQEvent) -> None:
    client.execute(
        """
        INSERT INTO dlq_events (
            original_topic,
            original_offset,
            original_timestamp,
            failure_reason,
            failure_detail,
            raw_payload,
            failed_at
        ) VALUES
        """,
        [
            (
                event.original_topic,
                event.original_offset,
                event.original_timestamp,
                event.failure_reason,
                event.failure_detail,
                event.raw_payload,
                event.failed_at,
            )
        ],
    )


def _replay_command(topic: str) -> str:
    script = Path(__file__).resolve()
    return f"python {script} replay --topic {topic}"


def _send_slack_alert(event: DLQEvent, error_detail: str) -> None:
    webhook_url = _env("SLACK_WEBHOOK_URL", "")
    if not webhook_url:
        logger.warning("SLACK_WEBHOOK_URL not set; skipping Slack alert")
        return

    failed_at_iso = event.failed_at.astimezone(timezone.utc).isoformat()
    text = (
        f":rotating_light: *DLQ sink failure*\n"
        f"*Topic:* `{event.original_topic}`\n"
        f"*Offset:* `{event.original_offset}`\n"
        f"*Error:* {error_detail}\n"
        f"*Time:* {failed_at_iso}\n"
        f"*Replay:* `{_replay_command(event.original_topic)}`"
    )
    try:
        response = requests.post(
            webhook_url,
            json={"text": text},
            timeout=_slack_request_timeout_sec(),
        )
        response.raise_for_status()
    except requests.RequestException:
        logger.exception("Failed to send Slack alert for topic=%s", event.original_topic)


def handle_sink_failure(event: DLQEvent) -> None:
    """Persist sink failure and page operators; never abort the consumer loop."""
    try:
        client = connect_clickhouse()
        _insert_dlq_event(client, event)
    except _CLICKHOUSE_WRITE_ERRORS:
        logger.exception(
            "Failed to write sink_failure to ClickHouse: topic=%s offset=%s",
            event.original_topic,
            event.original_offset,
        )

    _send_slack_alert(event, event.failure_detail)
    logger.error(
        "Sink failure: topic=%s offset=%s detail=%s",
        event.original_topic,
        event.original_offset,
        event.failure_detail,
    )


def handle_validation_failure(event: DLQEvent) -> None:
    """Persist validation failure without Slack noise."""
    try:
        client = connect_clickhouse()
        _insert_dlq_event(client, event)
    except _CLICKHOUSE_WRITE_ERRORS:
        logger.exception(
            "Failed to write validation_failure to ClickHouse: topic=%s offset=%s",
            event.original_topic,
            event.original_offset,
        )

    logger.warning(
        "Validation failure: topic=%s offset=%s detail=%s",
        event.original_topic,
        event.original_offset,
        event.failure_detail,
    )


def handle_deserialisation_failure(event: DLQEvent) -> None:
    """
    Log only — Schema Registry + Avro/JSON converters prevent deserialisation errors
    in production. Seeing this reason means Registry is down or converter config drifted.
    """
    logger.warning(
        "Deserialisation failure: topic=%s offset=%s detail=%s",
        event.original_topic,
        event.original_offset,
        event.failure_detail,
    )


def _route_event(event: DLQEvent) -> None:
    reason = event.failure_reason.strip().lower().replace("-", "_")
    if reason in {FAILURE_SINK, "sink"}:
        handle_sink_failure(event)
    elif reason in {FAILURE_VALIDATION, "validation"}:
        handle_validation_failure(event)
    elif reason in {FAILURE_DESERIALISATION, "deserialization_failure", "deserialization"}:
        handle_deserialisation_failure(event)
    else:
        logger.warning("Unknown failure_reason=%r; routing as validation", event.failure_reason)
        handle_validation_failure(event)


def consume(consumer: Consumer) -> None:
    """Poll DLQ topic, dispatch by failure_reason, commit after each handled message."""
    logger.info("DLQ consumer started on topic=%s", DLQ_TOPIC)
    try:
        while True:
            message = consumer.poll(timeout=1.0)
            if message is None:
                continue
            if message.error():
                if message.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(message.error())

            try:
                event = _parse_dlq_message(message.value())
                _route_event(event)
            except (json.JSONDecodeError, KeyError, ValueError, TypeError):
                logger.exception("Malformed DLQ message at offset=%s", message.offset())
            except _CONSUME_HANDLER_ERRORS:
                logger.exception("Unexpected error handling DLQ offset=%s", message.offset())
            finally:
                consumer.commit(message=message, asynchronous=False)
    except KeyboardInterrupt:
        logger.info("DLQ consumer shutting down (KeyboardInterrupt)")


def replay(topic: str) -> None:
    """
  Republish unreplayed raw payloads to the original Kafka topic.

  replayed_at is updated only after a successful produce — not before.
  """
    client = connect_clickhouse()
    rows = client.execute(
        """
        SELECT
            original_topic,
            original_offset,
            original_timestamp,
            failure_reason,
            failure_detail,
            raw_payload,
            failed_at
        FROM dlq_events
        WHERE original_topic = %(topic)s
          AND replayed_at IS NULL
        ORDER BY original_timestamp ASC
        """,
        {"topic": topic},
    )

    producer = Producer({"bootstrap.servers": _kafka_bootstrap()})
    replayed = 0
    failed = 0

    for row in rows:
        event = DLQEvent(
            original_topic=str(row[0]),
            original_offset=int(row[1]),
            original_timestamp=row[2],
            failure_reason=str(row[3]),
            failure_detail=str(row[4]),
            raw_payload=str(row[5]),
            failed_at=row[6],
        )
        try:
            producer.produce(
                topic=event.original_topic,
                value=event.raw_payload.encode("utf-8"),
            )
            producer.flush(timeout=_kafka_producer_flush_timeout_sec())
            client.execute(
                """
                ALTER TABLE dlq_events
                UPDATE replayed_at = now()
                WHERE original_topic = %(topic)s
                  AND original_offset = %(offset)s
                  AND failed_at = %(failed_at)s
                  AND replayed_at IS NULL
                """,
                {
                    "topic": event.original_topic,
                    "offset": event.original_offset,
                    "failed_at": event.failed_at,
                },
            )
            replayed += 1
        except _REPLAY_ERRORS:
            failed += 1
            logger.exception(
                "Replay failed for topic=%s offset=%s",
                event.original_topic,
                event.original_offset,
            )

    logger.info("%s events replayed, %s failed", replayed, failed)


def _create_dlq_consumer() -> Consumer:
    # earliest: DLQ is append-only audit log — new consumer groups must not skip history.
    auto_offset_reset = _env("KAFKA_DLQ_AUTO_OFFSET_RESET", "earliest")
    return Consumer(
        {
            "bootstrap.servers": _kafka_bootstrap(),
            "group.id": CONSUMER_GROUP,
            "auto.offset.reset": auto_offset_reset,
            "enable.auto.commit": False,
        }
    )


def main() -> None:
    load_dotenv()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    parser = argparse.ArgumentParser(description="DLQ consumer and replay for CDC pipeline")
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("consume", help="Consume dead_letter_queue and route failures")

    replay_parser = subparsers.add_parser("replay", help="Replay DLQ rows to original topic")
    replay_parser.add_argument("--topic", required=True, help="original_topic to replay")

    args = parser.parse_args()

    if args.command == "consume":
        consumer = _create_dlq_consumer()
        consumer.subscribe([DLQ_TOPIC])
        try:
            consume(consumer)
        finally:
            consumer.close()
    elif args.command == "replay":
        replay(args.topic)
    else:
        parser.error(f"Unknown command: {args.command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
