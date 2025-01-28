# Future Work

## Stage 1 — Identified Improvements

**Automated verification script:** Verification was done manually with psql queries and log output. A `scripts/verify_stage1.py` should assert all four tables exist, seed counts are exactly 1000 customers and 200 products, `wal_level=logical` is active, and one complete order workflow runs cleanly — exit code 0 on pass, 1 on fail.

**Port as environment variable:** Port 5433 is hardcoded in `docker-compose.yml` as a workaround for a local port conflict. Should be `${POSTGRES_HOST_PORT:-5432}:5432` so anyone cloning on a machine where 5432 is free doesn't need to edit the compose file.

**Load generator benchmark output:** `load_generator.py` logs throughput to stdout but saves nothing. Normal and flash_sale modes should write achieved TPS, error count, and transaction mix to `benchmarks/stage1_load_summary.json` to give a pre-CDC baseline for future latency comparisons.

**Seed reproducibility:** Faker and random calls run without a fixed seed so two runs produce different data. A `--random-seed` CLI argument would make runs fully reproducible across machines, which matters when downstream analytical work references specific data patterns.

---

## Stage 2 — Identified Improvements

**Avro converter via custom Debezium image:** The current setup uses JSON converters because Avro classes are absent from the base `debezium/connect:2.4` image. A `Dockerfile` extending the base image and installing the Confluent Avro converter plugin would restore Avro serialization, enabling Schema Registry enforcement and smaller message payloads on the Kafka topics.

**Topic pre-creation script:** Topics are currently auto-created by Debezium on first connect. A `scripts/create_topics.sh` that pre-creates all four CDC topics with explicit partition counts and replication factors would make the setup production-representative and remove the dependency on `KAFKA_AUTO_CREATE_TOPICS_ENABLE: true`.

**Publication and slot cleanup in `connector_setup.sh`:** The setup script does not clean up the Postgres replication slot or publication on re-registration. A full reset sequence (`DROP PUBLICATION`, `pg_drop_replication_slot`) should be added as an optional `--reset` flag so the cleanup step is codified rather than manual.

**Connector credentials via environment variables:** `database.user` and `database.password` are hardcoded strings in `connector_config.json`. These should be loaded from `.env` via a templating step or Kafka Connect's FileConfigProvider so credentials are never committed to the repository.

**Kafka internal bootstrap address in `.env`:** Flink will run inside Docker and cannot reach Kafka on `localhost:9092` — it needs `kafka:29092`. A `KAFKA_INTERNAL_BOOTSTRAP_SERVERS=kafka:29092` variable has been added to `.env` for use by in-container services. The existing `KAFKA_BOOTSTRAP_SERVERS=localhost:9092` remains for host-side tooling.

---

## Stage 3 — Future Work

**Flash sale latency optimisation:** End-to-end latency degrades 6.4x under flash sale load (p50: 982ms normal vs 6,249ms flash sale). Three root causes identified: (1) 30s Flink checkpoint interval means Kafka sink commits batch up to 30s late — reduce to 5s; (2) ClickHouse Kafka Engine default poll interval ~500ms — add `kafka_max_block_size=1000`; (3) single Kafka partition limits parallelism — increase to 3 partitions with matching Flink parallelism.

**Flink event-time windowing for order_metrics:** Current implementation uses a ClickHouse Materialized View as a workaround for PROCTIME() window closing behaviour. Production implementation should use Kafka message rowtime as the event-time attribute with a 5-second watermark for late CDC events.

**Debezium Prometheus metrics:** Kafka Connect does not expose a Prometheus endpoint by default. The JMX exporter agent needs mounting into the Debezium container with a config file mapping connector metrics to Prometheus format. Until then the kafka-connect Prometheus target shows DOWN.

**Flink order_metrics.py removal or replacement:** The file exists but is superseded by the ClickHouse MV. Either remove it to avoid confusion or replace the PROCTIME() approach with event-time windowing and reactivate it as the primary metrics source.

---

## Stage 4 — Identified Improvements

**Full DLQ replay integration test:** Test 3 was redesigned to use connector pause/resume after the original DDL-based failure approach did not work as expected. A dedicated integration test that artificially injects malformed events into the DLQ topic and verifies replay() reprocesses them in order with correct replayed_at timestamps would give better coverage of the replay path.

**DOCKER_HOST as documented environment variable:** The desktop socket path is currently set as a hardcoded default in the test file. It should be documented in .env.example with a comment explaining how to find the correct path on any machine using docker context ls.

**Slack alert integration test:** handle_sink_failure() sends a Slack alert but this is only verified by inspection. A test that stubs the webhook URL and asserts the payload contains the topic, offset, and replay command would catch regressions silently breaking the on-call alert.

**Connector auto-recovery configuration documented:** Test 2 verified that Debezium reconnects to Kafka automatically after a broker restart. The specific connector configuration properties that enable this behaviour are not documented anywhere in the repo. They should be added to debezium/CONNECTOR_NOTES.md so the behaviour is explainable without running the test.

## Stage 5 — Identified Improvements

**Custom Airflow Docker image with dbt pre-installed:** dbt is currently installed via `_PIP_ADDITIONAL_REQUIREMENTS` on every container startup, adding 2-3 minutes to each restart. A custom Dockerfile baking in `dbt-core` and `dbt-clickhouse` would make restarts instant and is the correct production pattern.

**`fct_revenue_hourly` as incremental model:** Currently materialised as a full-rebuild table every five minutes against an ever-growing `order_metrics_per_minute` source. Should be `materialized='incremental'` with an `updated_at` or `window_start` watermark to avoid the full scan growing with data volume.

**dbt source freshness alerting to Slack:** The Airflow DAG logs freshness failures but does not alert. A Slack webhook notification when sources exceed the error threshold would catch pipeline stalls faster than waiting for the next manual check of the Airflow UI.

**Singular test `assert_flash_sale_orders_exist` should be skipped outside flash sale windows:** The test currently fails on every run until flash sale data exists, requiring `|| true` in the Airflow DAG to prevent DAG failure. A dbt `--exclude` flag or a conditional `{% if var('run_flash_sale_tests', false) %}` wrapper would make the failure meaningful when it occurs rather than expected noise.

**`seconds_in_current_status` column is semantically imprecise:** `dateDiff('second', created_at, updated_at)` measures time from order creation to last update, not time spent in the current status. A correct implementation would require capturing status transition timestamps, which would need a separate status history table fed by CDC. The current column is useful as a proxy but should not be described as time-in-status in documentation.

**Shared ClickHouse connection pool for analysis scripts:** `run_checkpoint.py` and `flash_sale_analysis.py` both create their own connections on each invocation. A shared connection pool managed at the Airflow task level would reduce connection overhead during the five-minute DAG runs.