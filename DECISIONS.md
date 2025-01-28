# Architecture Decisions

## Stage 1 — Postgres Source and Load Generator

**Single file load generator over a package:** All three modes (seed, normal, flash_sale) live in `database/load_generator.py` alongside the schema, keeping the source layer self-contained and fully walkable without jumping between files.

**Port 5433 over 5432:** Port 5432 was held by a zombie Docker proxy on the dev machine. Remapped to 5433 permanently to avoid the same conflict mid-demo — container still runs on 5432 internally.

**WAL flags in docker-compose.yml command section over postgresql.conf:** Three flags don't justify a separate config file. Inline in the compose service definition they're immediately visible with comments explaining what breaks without each one.

**Feature branch per stage with --no-ff merge:** Preserves branch topology in `git log --graph`. Both `GIT_AUTHOR_DATE` and `GIT_COMMITTER_DATE` must be set on every commit — setting one without the other caused multiple history rebuilds during this stage.

**docker compose v2 over docker-compose v1:** v1 is end-of-life. Using `docker compose exec` by service name avoids hardcoded container names breaking on different machines.

---

## Stage 2 — Kafka, Schema Registry, Debezium

**JSON converters over Avro:** Neither the Confluent nor Apicurio Avro converter is bundled in `debezium/connect:2.4`. Switching to `org.apache.kafka.connect.json.JsonConverter` unblocked registration immediately and avoids a custom Docker build. Production pipelines should use Avro with Schema Registry for schema evolution guarantees and smaller message size — documented in `CONNECTOR_NOTES.md` as a known trade-off.

**`KAFKA_AUTO_CREATE_TOPICS_ENABLE: true` over manual pre-creation:** Debezium needs to create CDC topics and internal Connect topics on startup. Allowing auto-creation keeps the setup self-contained — no separate topic creation step required. Production deployments should pre-create topics with explicit partition counts and replication factors.

**`kafka-broker-api-versions` healthcheck over port check:** A TCP connection to port 9092 succeeding does not mean Kafka is ready to serve producer requests. The API versions check verifies the broker can actually respond to client negotiation, which is the readiness condition Debezium actually needs.

**`curl -sf` over `curl -s` for connector existence check:** `curl -s` exits 0 on any HTTP response including 404. Without `-f`, the idempotent delete-if-exists check in `connector_setup.sh` always evaluated true and fired a DELETE on every run, including the first. `-f` makes curl exit non-zero on 4xx responses so the block only executes when the connector genuinely exists.

**Postgres host port remapped to 5434:** Port 5433 (the Stage 1 workaround) was occupied during Stage 2 bring-up by another local process. Remapped to 5434 permanently. The container continues to run on 5432 internally — no change to inter-service communication.

**Publication and replication slot cleanup in reset runbook:** After a full stack reset, `dbz_publication` persisted in Postgres and blocked connector restart. `DROP PUBLICATION IF EXISTS dbz_publication` must be run before re-registering the connector on any environment that has previously run Debezium against the same database.

---

## Stage 3 — Architecture Decisions

**Raw format over debezium-json:** Debezium's JsonConverter emits a `{"schema":{...},"payload":{...}}` envelope that Flink's debezium-json deserialiser cannot parse, causing a silent NullPointerException with zero records processed. Raw format reads the full message as STRING; JSON_VALUE() extracts fields manually.

**Kafka intermediary over direct ClickHouse sink:** Flink's JDBC connector has no ClickHouse dialect. HTTP sink requires custom code with manual retry and backpressure handling. Kafka intermediary uses the existing Kafka connector, decouples Flink from ClickHouse, and lets ClickHouse Kafka Engine consume at its own pace with offset tracking.

**flink-sql-connector-kafka 3.0.2-1.18 over 1.17.2:** Version 1.17.2 is missing the shaded Guava dependency required by the Kafka sink committer. The 3.0.2 SQL connector bundles all transitive dependencies, eliminating the NoClassDefFoundError.

**ClickHouse Materialized View for order_metrics over Flink windowing:** PROCTIME() tumbling windows in Flink Table API do not emit results when the source goes idle between bursts. ClickHouse MV fires on every INSERT to orders_current and is operationally simpler. Flink event-time windowing with Kafka rowtime metadata is the correct production approach and is documented in future work.

**Explicit datasource UIDs in provisioning YAML:** Grafana auto-generates UIDs when none are specified, causing dashboard JSON references to break across container restarts. Pinning UIDs in datasources.yml makes dashboards reproducible.

**ClickHouse depends_on kafka healthy:** Kafka Engine tables attempt broker connection on startup. Without the dependency, ClickHouse starts before Kafka is ready and fails its healthcheck.

**CLICKHOUSE_NATIVE_PORT=19000 in .env:** The host maps ClickHouse native protocol to 19000 to avoid conflicts. The e2e benchmark uses clickhouse-driver which connects via native protocol; hardcoding 9000 caused an UnexpectedPacketFromServerError.

---

## Stage 4 — Dead Letter Queue and Failure Recovery Tests

**Docker SDK container lookup by label over name:** Container names include the project folder name as a prefix which varies between machines. Filtering by com.docker.compose.service label is stable regardless of folder name or Docker context.

**Explicit DOCKER_HOST over docker.from_env():** The dev machine runs two Docker contexts. from_env() picks the wrong one silently. Reading DOCKER_HOST from the environment with the correct socket as the default makes the test portable without hardcoding a path in the code itself.

**requests data= over json= for Schema Registry calls:** The json= parameter in the requests library overrides any manually supplied Content-Type header. Schema Registry requires application/vnd.schemaregistry.v1+json — using data=json.dumps(payload) is the only way to preserve that header.

**Connector pause/resume over DDL-triggered failure for Test 3:** ALTER TABLE on a nullable column does not cause Debezium to leave the RUNNING state — it reads the WAL not the schema. Pause/resume via the Kafka Connect REST API tests the same recovery behaviour without relying on an incorrect assumption about connector failure modes.

**Retry loop over single snapshot for count reconciliation:** A count taken immediately after connector recovery gives a false mismatch while the backlog drains. Polling every 5 seconds up to 30 seconds and stopping when counts match gives a stable result without adding an arbitrary fixed sleep.

**replay() marks replayed_at after publish not before:** Marking before publishing means a crash mid-replay silently drops events with no way to identify which ones need reprocessing. Marking after means a crash leaves events with replayed_at IS NULL so they will be picked up on the next replay run.

## Stage 5 — dbt, Great Expectations, Airflow

**dbt-clickhouse with delete+insert incremental strategy over merge:** ClickHouse does not support in-place UPDATE so `delete+insert` is the only viable incremental strategy. Merge strategy is unavailable for ClickHouse adapter at this version.

**post_hook for tombstone cleanup over filtering in the model:** Deleted rows with `is_deleted = 1` are filtered from the SELECT, so they never pass through the incremental watermark check and dbt never generates a delete for them. A `post_hook` DELETE after each run is the only way to remove them without a full refresh.

**FINAL in subquery for JOIN over FINAL on JOIN clause:** ClickHouse only supports `FINAL` on the main `FROM` table. The customers join in `fct_order_performance` uses a subquery with `FINAL` applied inside it — the only syntax ClickHouse accepts.

**Separate base CTEs for cancellation rate in mart_flash_sale_analysis:** Filtering `seconds_to_confirm IS NOT NULL` globally excludes all cancelled orders from cancellation rate calculation. Separate CTEs — one for all orders, one for confirmed orders only — are required to compute both metrics correctly from the same source.

**Airflow over APScheduler or cron for dbt orchestration:** Airflow provides a UI showing task-level logs, retry history, and DAG run status alongside Flink UI and Grafana. APScheduler and cron give none of this visibility. The RAM overhead (~1.5GB) is acceptable on the dev machine running Linux with Docker native.

**Airflow under docker-compose profile over always-on service:** Airflow is not needed during benchmarking or failure tests. Running it under `--profile airflow` means it does not consume RAM during normal stack operation and can be started only when the orchestration layer needs to be demonstrated.

**`_PIP_ADDITIONAL_REQUIREMENTS` for dbt install in Airflow over custom Dockerfile:** A custom Dockerfile would require a separate build step and image registry. For a single-machine portfolio project, installing dbt at container startup via `_PIP_ADDITIONAL_REQUIREMENTS` is sufficient and keeps the setup to a single `docker-compose.yml` file.

**Port 9000 for ClickHouse native TCP inside Docker over port 19000:** Port 19000 is the host-mapped port only accessible from outside the Docker network. Services inside Docker must use port 9000, the internal native TCP port. This distinction matters for `great_expectations/run_checkpoint.py` and `analysis/flash_sale_analysis.py` which both use `clickhouse-driver`.

**dbt 1.8.0 over 1.7.4:** Version 1.7.4 is incompatible with Python 3.12 due to a protobuf conflict. 1.8.0 resolves this and is the minimum version that works cleanly in the project environment.

**`metaplane/dbt_expectations` over `calogica/dbt_expectations`:** The `calogica` package is deprecated. `metaplane/dbt_expectations` is the maintained fork with identical macro signatures — a drop-in replacement requiring only a packages.yml change.