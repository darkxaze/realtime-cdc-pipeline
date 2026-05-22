# Postgres CDC connector — configuration notes

`connector_config.json` is posted to Debezium Connect as-is (JSON has no comments). Register after the stack is healthy:

```bash
curl -X POST -H "Content-Type: application/json" \
  --data @debezium/connector_config.json \
  http://localhost:8083/connectors
```

## Obvious / structural settings

| Setting | Role |
|--------|------|
| `connector.class` | Postgres logical replication via Debezium |
| `database.*` | Source DB connection (see hostname note below) |
| `database.server.name` | Logical name embedded in topic names and schema history |
| `table.include.list` | Only the four ecommerce tables we model in Flink/ClickHouse |
| `plugin.name` | `pgoutput` — native Postgres 10+ logical decoding (not `decoderbufs`) |
| `topic.prefix` | Kafka topic prefix: `ecommerce.<schema>.<table>` |
| `key.converter` / `value.converter` | JSON Connect converters (see Converter Configuration below) |

---

## Converter Configuration

**Why JSON converters instead of Avro?**

For this portfolio project, JSON converters are simpler and avoid
the complexity of Schema Registry integration during initial setup.

- key.converter: org.apache.kafka.connect.json.JsonConverter
- value.converter: org.apache.kafka.connect.json.JsonConverter
- schemas.enable: true on value converter (includes schema metadata)

**Production consideration:** Avro with Schema Registry would be
preferred for production due to better schema evolution, smaller
message size, and stronger typing. This is a known trade-off.

---

## `database.hostname`: `postgres` not `localhost`

Connect runs **inside** the Docker network. Service DNS names resolve to container IPs; `localhost` inside the Debezium container is the Connect JVM itself, not the Postgres container.

- **Use:** `postgres` — the `docker-compose.yml` service name on the internal port `5432`.
- **Reject:** `localhost` — connection refused or wrong process; CDC never starts despite a healthy Postgres on the host at `5434:5432`.

Host tools (`load_generator.py`, `psql` on the laptop) still use `localhost` and the published host port; only in-network clients use `postgres`.

---

## Hardcoded Credentials

`database.user` and `database.password` are hardcoded in connector_config.json for simplicity. Production deployments must externalize secrets via Kafka Connect's ConfigProvider interface, not committed JSON files.

---

## `decimal.handling.mode`: `double` not `precise`

Money columns (`orders.total_amount`, line prices) are `NUMERIC` in Postgres. Debezium can emit decimals as:

| Mode | Behaviour |
|------|-----------|
| `precise` | Avro `decimal` logical type (bytes + scale) |
| `double` | IEEE-754 double in the Avro payload |

**Chosen:** `double`.

**Rejected:** `precise`. With Flink 1.18 and Confluent Avro deserialisation, `precise` produced a `ClassCastException` on `total_amount`: the runtime expected a type compatible with Flink’s SQL `DECIMAL` path, but the in-flight value was the Avro decimal representation (logical-type bytes). That is an encoding/serializer mismatch between Debezium’s Avro encoder and Flink’s converter, not bad source data.

**Trade-off:** doubles are not exact for currency; acceptable here because amounts are bounded demo values and the pipeline prioritises end-to-end CDC over accounting-grade decimal semantics. For production money, fix the serializer chain or use a string/decimal type Flink accepts — do not assume `precise` works without validating the full Flink → ClickHouse path.

---

## `tombstones.on.delete`: `true`

On `DELETE`, Kafka can receive:

1. A **delete event** (payload describes the removed row), and optionally  
2. A **tombstone**: a record with the same key and **null value**.

**Chosen:** `true` — emit tombstones after deletes.

**Why:** Many stream processors (Flink upsert/kafka changelog, compacted topics, some ClickHouse materialisations) treat **key + null value** as “this key no longer exists.” Without tombstones, consumers may keep the last upsert forever and **deletes look like no-ops** — the core failure mode this project addresses (cancelled orders still counted in analytics).

**Rejected:** `false` — deletes might only appear as a prior-state envelope; compacted topics and tables keyed by primary key never remove the row.

---

## `heartbeat.interval.ms`: `1000`

Debezium injects **heartbeat** records on the replication stream when there is no table activity. They carry no business data but advance consumer lag and LSN bookkeeping.

**Chosen:** `1000` ms (1 s).

**Why:** At higher TPS (e.g. flash sale ~50 TPS), Postgres **WAL segment rotation** can pause the logical replication stream for tens of seconds with **no errors** and connector status still `RUNNING`. Monitoring and tests then cannot tell “healthy quiet period” from “stuck connector.” A 1 s heartbeat produces regular traffic so lag and timestamps keep moving during WAL maintenance.

**Rejected:** default (often much higher or off) — longer gaps look like failures; triggered false alerts and confused recovery benchmarks.

This does not remove WAL rotation cost; it makes stalls **visible** and bounded for observability.

---

## `publication.autocreate.mode`: `filtered` not `all_tables`

Postgres logical replication uses a **publication** listing which tables send changes. Debezium can create it automatically.

| Mode | Behaviour |
|------|-----------|
| `all_tables` | Publication includes every table in the database |
| `filtered` | Publication includes only tables matching `table.include.list` |

**Chosen:** `filtered`.

**Why:** We only CDC four tables. A publication over the whole database would:

- Stream noise from tables we do not model (higher Kafka volume, wider blast radius).
- Complicate schema changes and slot retention (more WAL held if something touches non-CDC tables).
- Diverge from the explicit allowlist in `table.include.list`.

**Rejected:** `all_tables` — simpler on day one, wrong for a bounded ecommerce schema and fixed downstream contracts.

---

## `snapshot.mode`: `initial`

Controls whether Debezium reads existing rows before streaming WAL events.

| Mode | Behaviour |
|------|-----------|
| `initial` | Snapshot included tables once, then continuous CDC |
| `never` | Only changes after connector start |
| `when_needed` / others | Conditional snapshot behaviour |

**Chosen:** `initial`.

**Why:** On first connector deploy, ClickHouse/Flink must **match current Postgres row counts** before incremental events make sense. Without a snapshot, only new inserts/updates after connect appear — historical customers, products, and open orders are missing and integrity checks (24 h row count parity) fail immediately.

**Rejected:** `never` for production cutover — correct only if another bulk load already hydrated the sink and you only want deltas from a known offset.

Re-snapshot behaviour on connector restart is governed by Debezium offset storage; `initial` applies when no viable offset exists for the slot.

---

## Related operational notes

- **Credentials** in JSON match the demo `.env` / `init.sql` user; override via secrets in real deployments, not committed production passwords.
- **`KAFKA_AUTO_CREATE_TOPICS_ENABLE: true`** in Compose — Debezium auto-creates CDC topics (`ecommerce.*`) and Connect internal topics on startup. Production deployments should pre-create topics with explicit replication factors and partition counts.
- **Slot cleanup** after tests — a stalled connector holds the replication slot and WAL on disk (see project bug notes in `CLAUDE.md`).
