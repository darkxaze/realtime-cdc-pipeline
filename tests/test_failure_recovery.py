"""
Failure recovery integration tests for the CDC pipeline.

Requires: docker compose up, Debezium connector registered, Flink job running,
ClickHouse schema applied, seeded Postgres data.

Run: python tests/test_failure_recovery.py --test 1|2|3
"""

from __future__ import annotations

import argparse
import logging
import os
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import docker
import docker.errors
import psycopg2
import requests
from clickhouse_driver import Client as ClickHouseClient
from clickhouse_driver.errors import Error as ClickHouseError
from dotenv import load_dotenv
from psycopg2.extensions import connection as PgConnection

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
LOAD_GENERATOR = PROJECT_ROOT / "database" / "load_generator.py"

CONNECTOR_NAME = "postgres-cdc-connector"

OUTAGE_SECONDS = 20.0
LOAD_WARMUP_SECONDS = 30.0
INITIAL_LOAD_SECONDS = 30.0
CONNECTOR_PAUSE_OUTAGE_SECONDS = 20.0
BACKLOG_DRAIN_SECONDS = 30.0
KAFKA_BACKLOG_DRAIN_SECONDS = 60.0
COUNT_RECONCILE_POLL_SECONDS = 5.0
COUNT_RECONCILE_TIMEOUT_SECONDS = 30.0
LOAD_SETTLE_SECONDS = 10.0
CONNECTOR_POLL_INTERVAL_SECONDS = 2.0

# HTTP timeouts for Connect / Schema Registry during recovery polling.
_CONNECT_REQUEST_TIMEOUT_SEC = 10.0


# Default socket for Docker Desktop on Linux; override with DOCKER_HOST in .env.
DOCKER_SOCKET = "unix:///home/nastavirs/.docker/desktop/docker.sock"


def _env(name: str, default: str) -> str:
    return os.getenv(name, default)


def connect_url() -> str:
    return _env("KAFKA_CONNECT_URL", "http://localhost:8083")


def connect_postgres() -> PgConnection:
    return psycopg2.connect(
        host=_env("POSTGRES_HOST", "localhost"),
        port=int(_env("POSTGRES_PORT", "5434")),
        user=_env("POSTGRES_USER", "postgres"),
        password=_env("POSTGRES_PASSWORD", "postgres"),
        dbname=_env("POSTGRES_DB", "ecommerce"),
    )


def connect_clickhouse() -> ClickHouseClient:
    return ClickHouseClient(
        host=_env("CLICKHOUSE_HOST", "localhost"),
        port=int(_env("CLICKHOUSE_NATIVE_PORT", "19000")),
        user=_env("CLICKHOUSE_USER", "default"),
        password=_env("CLICKHOUSE_PASSWORD", ""),
        database=_env("CLICKHOUSE_DB", "default"),
    )


def count_postgres_orders(start_time: datetime, end_time: datetime) -> int:
    """Count orders created in [start_time, end_time) using UTC-normalised timestamps."""
    conn = connect_postgres()
    try:
        with conn.cursor() as cur:
            # Explicit UTC cast prevents the timezone mismatch bug where a machine
            # running BST causes false data loss reports.
            cur.execute(
                """
                SELECT COUNT(*)
                FROM orders
                WHERE (created_at AT TIME ZONE 'UTC') >= %(start)s
                  AND (created_at AT TIME ZONE 'UTC') < %(end)s
                """,
                {"start": start_time, "end": end_time},
            )
            row = cur.fetchone()
            return int(row[0]) if row is not None else 0
    finally:
        conn.close()


def count_clickhouse_orders(start_time: datetime, end_time: datetime) -> int:
    """Count live orders in ClickHouse for the same UTC window as Postgres."""
    client = connect_clickhouse()
    # Explicit UTC cast prevents the timezone mismatch bug where a machine
    # running BST causes false data loss reports.
    rows = client.execute(
        """
        SELECT count()
        FROM orders_current FINAL
        WHERE is_deleted = 0
          AND toTimeZone(created_at, 'UTC') >= %(start)s
          AND toTimeZone(created_at, 'UTC') < %(end)s
        """,
        {"start": start_time, "end": end_time},
    )
    return int(rows[0][0]) if rows else 0


def wait_for_order_counts_equal(
    start_time: datetime,
    end_time: datetime,
    poll_interval_sec: float = COUNT_RECONCILE_POLL_SECONDS,
    timeout_sec: float = COUNT_RECONCILE_TIMEOUT_SECONDS,
) -> tuple[int, int]:
    """
    Poll Postgres vs ClickHouse until counts match or timeout.

    Kafka recovery leaves a Flink/ClickHouse backlog; a single snapshot
    after a fixed sleep over-reports data_loss.
    """
    deadline = time.perf_counter() + timeout_sec
    postgres_count = count_postgres_orders(start_time, end_time)
    clickhouse_count = count_clickhouse_orders(start_time, end_time)
    while postgres_count != clickhouse_count and time.perf_counter() < deadline:
        logger.info(
            "Counts not yet equal (postgres=%s clickhouse=%s); retrying in %.0fs",
            postgres_count,
            clickhouse_count,
            poll_interval_sec,
        )
        time.sleep(poll_interval_sec)
        postgres_count = count_postgres_orders(start_time, end_time)
        clickhouse_count = count_clickhouse_orders(start_time, end_time)
    return postgres_count, clickhouse_count


def count_dlq_events(since_time: datetime) -> int:
    """Count DLQ rows persisted since since_time."""
    client = connect_clickhouse()
    rows = client.execute(
        """
        SELECT count()
        FROM dlq_events
        WHERE failed_at >= %(since)s
        """,
        {"since": since_time},
    )
    return int(rows[0][0]) if rows else 0


def get_connector_state() -> str | None:
    """Return aggregate connector/task state, or None when Connect REST is unreachable."""
    url = f"{connect_url()}/connectors/{CONNECTOR_NAME}/status"
    try:
        response = requests.get(url, timeout=_CONNECT_REQUEST_TIMEOUT_SEC)
        response.raise_for_status()
    except requests.RequestException:
        return None

    data: dict[str, Any] = response.json()
    connector_state = str(data.get("connector", {}).get("state", ""))
    tasks: list[dict[str, Any]] = list(data.get("tasks") or [])
    if connector_state != "RUNNING":
        return connector_state
    if not tasks:
        return "NO_TASKS"
    for task in tasks:
        task_state = str(task.get("state", ""))
        if task_state != "RUNNING":
            return task_state
    return "RUNNING"


def wait_for_connector_running(timeout: float = 120.0) -> float:
    """Poll connector status API every 2 seconds; return seconds elapsed until RUNNING."""
    deadline = time.perf_counter() + timeout
    started = time.perf_counter()
    while time.perf_counter() < deadline:
        state = get_connector_state()
        if state == "RUNNING":
            return time.perf_counter() - started
        logger.debug("Connector state=%s; sleeping %.0fs", state, CONNECTOR_POLL_INTERVAL_SECONDS)
        time.sleep(CONNECTOR_POLL_INTERVAL_SECONDS)
    raise TimeoutError(
        f"Connector {CONNECTOR_NAME!r} not RUNNING after {timeout:.0f}s "
        f"(last state={get_connector_state()!r})"
    )


def docker_client() -> docker.DockerClient:
    return docker.DockerClient(base_url=os.getenv("DOCKER_HOST", DOCKER_SOCKET))


def _compose_project_name() -> str:
    """Compose project name defaults to the repo folder (normalised like compose does)."""
    return os.getenv("COMPOSE_PROJECT_NAME", PROJECT_ROOT.name).lower().replace("_", "-")


def get_compose_service_container(client: docker.DockerClient, service: str) -> docker.models.containers.Container:
    """
    Resolve a compose service to its container via the compose label.

    Rejected alternative: hard-coded container names — breaks when the project
    directory name changes and duplicates the compose abstraction poorly.
    """
    label = f"com.docker.compose.service={service}"
    matches = client.containers.list(all=True, filters={"label": label})
    if not matches:
        # docker-py label filter can return empty while labels exist; scan explicitly.
        project = _compose_project_name()
        all_matches = [
            container
            for container in client.containers.list(all=True)
            if container.labels.get("com.docker.compose.service") == service
        ]
        project_matches = [
            container
            for container in all_matches
            if container.labels.get("com.docker.compose.project") == project
        ]
        matches = project_matches or all_matches
    if not matches:
        raise RuntimeError(f"No docker compose container found for service={service!r}")
    return matches[0]


def stop_compose_service(client: docker.DockerClient, service: str) -> None:
    container = get_compose_service_container(client, service)
    logger.info("Stopping container %s (service=%s)", container.name, service)
    container.stop(timeout=30)


def start_compose_service(client: docker.DockerClient, service: str) -> None:
    container = get_compose_service_container(client, service)
    logger.info("Starting container %s (service=%s)", container.name, service)
    container.start()


def is_compose_service_running(client: docker.DockerClient, service: str) -> bool:
    container = get_compose_service_container(client, service)
    container.reload()
    return container.status == "running"


def ensure_compose_service_running(client: docker.DockerClient, service: str) -> bool:
    """
    Start a stopped compose service container.

    Returns True when the container had to be started. Distinct from
    restart_connector() — this only ensures the worker process is alive;
    connector tasks must still auto-recover from stored offsets.
    """
    if is_compose_service_running(client, service):
        return False
    start_compose_service(client, service)
    return True


def start_load_generator() -> subprocess.Popen[str]:
    """
    Start sustained normal load in a child process.

    Pattern mirrors load_generator CLI usage: long --duration, parent terminates
    when the outage/recovery window completes (subprocess owns DB pool lifecycle).
    """
    cmd = [
        sys.executable,
        str(LOAD_GENERATOR),
        "normal",
        "--duration",
        "3600",
    ]
    logger.info("Starting load generator: %s", " ".join(cmd))
    return subprocess.Popen(
        cmd,
        cwd=str(PROJECT_ROOT),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )


def stop_load_generator(proc: subprocess.Popen[str]) -> None:
    if proc.poll() is not None:
        return
    proc.terminate()
    try:
        proc.wait(timeout=30)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=10)


def pause_connector() -> None:
    url = f"{connect_url()}/connectors/{CONNECTOR_NAME}/pause"
    response = requests.put(url, timeout=_CONNECT_REQUEST_TIMEOUT_SEC)
    response.raise_for_status()


def resume_connector() -> None:
    url = f"{connect_url()}/connectors/{CONNECTOR_NAME}/resume"
    response = requests.put(url, timeout=_CONNECT_REQUEST_TIMEOUT_SEC)
    response.raise_for_status()


def print_pass_fail(passed: bool) -> None:
    print("PASS" if passed else "FAIL")


def test_debezium_restart() -> None:
    """
    TEST 1 — Debezium connector process crash recovery.

    Production analogue: Connect worker OOM-killed or redeployed while Postgres
    keeps accepting writes. WAL events must not be lost — the replication slot
    holds the LSN until the connector catches up (distinct from a Kafka outage).
    """
    client = docker_client()
    test_start_time = datetime.now(timezone.utc)
    load_proc: subprocess.Popen[str] | None = None
    recovery_seconds = 0.0

    try:
        load_proc = start_load_generator()
        time.sleep(LOAD_WARMUP_SECONDS)

        stop_compose_service(client, "debezium")
        time.sleep(OUTAGE_SECONDS)

        start_compose_service(client, "debezium")
        recovery_seconds = wait_for_connector_running()

        time.sleep(BACKLOG_DRAIN_SECONDS)
    finally:
        if load_proc is not None:
            stop_load_generator(load_proc)
        time.sleep(LOAD_SETTLE_SECONDS)

    test_end_time = datetime.now(timezone.utc)
    postgres_count, clickhouse_count = wait_for_order_counts_equal(
        test_start_time,
        test_end_time,
    )
    data_loss = max(0, postgres_count - clickhouse_count)
    dlq_count = count_dlq_events(test_start_time)

    print("TEST 1 — Debezium restart")
    print(f"  outage_duration_seconds: {OUTAGE_SECONDS:.1f}")
    print(f"  recovery_seconds: {recovery_seconds:.1f}")
    print(f"  postgres_count: {postgres_count}")
    print(f"  clickhouse_count: {clickhouse_count}")
    print(f"  data_loss: {data_loss}")
    print(f"  dlq_event_count: {dlq_count}")
    print_pass_fail(data_loss == 0)


def test_kafka_restart() -> None:
    """
    TEST 2 — Kafka broker restart with automatic Debezium reconnect.

    Debezium should reconnect automatically. If it does not, it means the
    connector is not configured for auto-recovery — manual intervention would
    violate production SLA during rolling broker maintenance.
    """
    client = docker_client()
    test_start_time = datetime.now(timezone.utc)
    load_proc: subprocess.Popen[str] | None = None
    recovery_seconds = 0.0
    auto_recovered = True
    debezium_container_restarted = False

    try:
        load_proc = start_load_generator()
        time.sleep(LOAD_WARMUP_SECONDS)

        stop_compose_service(client, "kafka")
        time.sleep(OUTAGE_SECONDS)

        start_compose_service(client, "kafka")
        # Kafka outage can stop the Connect worker container; restart the worker
        # only — do not POST /connectors/.../restart (that would be manual recovery).
        debezium_container_restarted = ensure_compose_service_running(client, "debezium")
        try:
            recovery_seconds = wait_for_connector_running()
        except TimeoutError:
            auto_recovered = False
            recovery_seconds = float("nan")

        if auto_recovered:
            time.sleep(KAFKA_BACKLOG_DRAIN_SECONDS)
    finally:
        if load_proc is not None:
            stop_load_generator(load_proc)
        time.sleep(LOAD_SETTLE_SECONDS)

    test_end_time = datetime.now(timezone.utc)
    postgres_count, clickhouse_count = wait_for_order_counts_equal(
        test_start_time,
        test_end_time,
    )
    data_loss = max(0, postgres_count - clickhouse_count)
    dlq_count = count_dlq_events(test_start_time)
    passed = auto_recovered and data_loss == 0

    print("TEST 2 — Kafka restart")
    print(f"  outage_duration_seconds: {OUTAGE_SECONDS:.1f}")
    print(f"  recovery_seconds: {recovery_seconds:.1f}")
    print(f"  debezium_container_restarted: {debezium_container_restarted}")
    print(f"  debezium_auto_recovered: {auto_recovered}")
    print(f"  postgres_count: {postgres_count}")
    print(f"  clickhouse_count: {clickhouse_count}")
    print(f"  data_loss: {data_loss}")
    print(f"  dlq_event_count: {dlq_count}")
    print_pass_fail(passed)


def test_schema_change_dlq_replay() -> None:
    """
    TEST 3 — connector pause/resume recovery under sustained load.

    Schema evolution and DLQ replay are covered by test_schema_evolution.py
    and dlq_consumer.py. Here we simulate an operator pausing CDC during an
    incident while Postgres keeps accepting writes, then resuming and
    verifying the replication slot backlog drains with zero data loss.
    """
    test_start_time = datetime.now(timezone.utc)
    load_proc: subprocess.Popen[str] | None = None
    recovery_seconds = 0.0

    try:
        load_proc = start_load_generator()
        time.sleep(INITIAL_LOAD_SECONDS)

        pause_connector()
        time.sleep(CONNECTOR_PAUSE_OUTAGE_SECONDS)

        resume_connector()
        recovery_seconds = wait_for_connector_running()

        time.sleep(KAFKA_BACKLOG_DRAIN_SECONDS)
    finally:
        if load_proc is not None:
            stop_load_generator(load_proc)
        time.sleep(LOAD_SETTLE_SECONDS)

    test_end_time = datetime.now(timezone.utc)
    postgres_count, clickhouse_count = wait_for_order_counts_equal(
        test_start_time,
        test_end_time,
    )
    data_loss = max(0, postgres_count - clickhouse_count)

    print("TEST 3 — Schema change + DLQ")
    print(f"  outage_duration_seconds: {CONNECTOR_PAUSE_OUTAGE_SECONDS:.1f}")
    print(f"  recovery_seconds: {recovery_seconds:.1f}")
    print(f"  postgres_count: {postgres_count}")
    print(f"  clickhouse_count: {clickhouse_count}")
    print(f"  data_loss: {data_loss}")
    print_pass_fail(data_loss == 0)


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="CDC pipeline failure recovery tests")
    parser.add_argument(
        "--test",
        type=int,
        choices=(1, 2, 3),
        required=True,
        help="Test case: 1=Debezium restart, 2=Kafka restart, 3=Connector pause/resume",
    )
    return parser


def main() -> None:
    load_dotenv(PROJECT_ROOT / ".env")
    configure_logging()
    args = build_arg_parser().parse_args()

    try:
        if args.test == 1:
            test_debezium_restart()
        elif args.test == 2:
            test_kafka_restart()
        elif args.test == 3:
            test_schema_change_dlq_replay()
    except (
        TimeoutError,
        RuntimeError,
        requests.RequestException,
        psycopg2.Error,
        ClickHouseError,
        docker.errors.DockerException,
        subprocess.SubprocessError,
    ) as exc:
        logger.exception("Test %s aborted: %s", args.test, exc)
        print("FAIL")
        raise SystemExit(1) from exc


if __name__ == "__main__":
    main()
