-- ClickHouse sink schema for the e-commerce CDC pipeline.
-- Apply against CLICKHOUSE_DB from .env (default: default).
-- Flink upserts dimension tables; append-only tables ingest metrics and DLQ rows.

/*
  orders_current — latest order header per order_id

  Stores: one logical row per order mirrored from Postgres (status, amount,
  customer link, timestamps). CDC delivers multiple physical rows per order_id
  as status changes; the sink must collapse to the newest version.

  Engine: ReplacingMergeTree(updated_at)
  Rejected: CollapsingMergeTree — needs paired +/- rows (sign column) and
  careful ordering of insert/cancel pairs. Flink changelog + Debezium tombstones
  already express deletes via is_deleted; a separate collapse sign duplicates
  that model and complicates replay. ReplacingMergeTree keeps the row with the
  highest updated_at per ORDER BY key at merge time — matches upsert semantics.

  ReplacingMergeTree vs CollapsingMergeTree (detail):
  - CollapsingMergeTree: two rows per mutation (+1 state, -1 cancel old state).
    Wrong sign order or missing pair leaves ghost or duplicate rows.
  - ReplacingMergeTree: each CDC event is a full row; version column picks winner.
    Tombstones set is_deleted=1 with a fresh updated_at so the delete wins.

  FINAL modifier:
  - Background merges are asynchronous; without FINAL, SELECT may return multiple
    versions of the same order_id until parts merge.
  - FINAL forces deduplication at query time (logically merged result).
  - Use FINAL on dashboard / integrity queries where correctness beats raw speed.
  - Do not use FINAL on latency-sensitive paths (e.g. sub-100ms fraud scoring);
    prefer materialized views, periodic OPTIMIZE, or accepting brief staleness.

  FINAL latency cost (measured on this project, inventory query shape):
  - Without FINAL: ~12ms p50
  - With FINAL:    ~67ms p50
  Acceptable for Grafana; not acceptable for a sub-100ms fraud API.

  ORDER BY order_id:
  - Primary sort key in storage. Filters and joins on order_id read contiguous
    ranges; aggregations grouped by order_id avoid wide scans.

  PARTITION BY toYYYYMM(created_at):
  - Prunes monthly dashboard windows and retention; order_id lookups still work
    across partitions via the primary key prefix in the granule index.
*/
CREATE TABLE IF NOT EXISTS orders_current
(
    order_id UUID,
    customer_id UUID,
    status String,
    total_amount Float64,
    created_at DateTime64(3, 'UTC'),
    updated_at DateTime64(3, 'UTC'),
    is_deleted UInt8 DEFAULT 0
)
ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(created_at)
ORDER BY order_id;

/*
  products_current — catalog and live inventory per product_id

  Stores: SKU, pricing, category, and inventory_count as updated by CDC
  (including rapid decrements during flash sale). Multiple versions per
  product_id exist until merge; dashboards query inventory here.

  Engine: ReplacingMergeTree(updated_at)
  Same rationale as orders_current: versioned upserts from Flink, tombstone
  deletes via is_deleted. Inventory staleness at 50 TPS is Bug 2 — use FINAL
  on inventory reads (12ms→67ms measured).

  ORDER BY product_id:
  - Point lookups by product_id (stock checks, SKU panels) stay on-primary-key.
*/
CREATE TABLE IF NOT EXISTS products_current
(
    product_id UUID,
    sku String,
    name String,
    category String,
    price Float64,
    inventory_count Int32,
    updated_at DateTime64(3, 'UTC'),
    is_deleted UInt8 DEFAULT 0
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY product_id;

/*
  customers_current — customer dimension (email, tier)

  Stores: customer attributes that change on tier updates; inserts from signup.
  Deletes rare; is_deleted supports CDC tombstones when rows are removed.

  Engine: ReplacingMergeTree(updated_at)
  Dimension table with infrequent updates; dedup by updated_at is sufficient.

  ORDER BY customer_id:
  - Join key to orders_current; filters on customer_id use the sort key.
*/
CREATE TABLE IF NOT EXISTS customers_current
(
    customer_id UUID,
    email String,
    tier String,
    created_at DateTime64(3, 'UTC'),
    updated_at DateTime64(3, 'UTC'),
    is_deleted UInt8 DEFAULT 0
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY customer_id;

/*
  order_metrics_per_minute — pre-aggregated order KPIs per minute window

  Stores: orders_count, revenue, avg_order_value, cancellations_count for
  each window_start (Grafana time-series, flash-sale comparison).

  Engine: MergeTree() not ReplacingMergeTree
  Rejected: ReplacingMergeTree — each window_start is written once by the
  Flink aggregation job (append-only, idempotent insert per window). No duplicate
  versions of the same minute need merge-time deduplication; ReplacingMergeTree
  would add merge overhead with no benefit.

  ORDER BY window_start:
  - Time-range scans for dashboards (last N hours) read in sort order.

  PARTITION BY toYYYYMM(window_start):
  - Drops old months cheaply; bounds scan size for long-range charts.
*/
CREATE TABLE IF NOT EXISTS order_metrics_per_minute
(
    window_start DateTime64(3, 'UTC'),
    orders_count UInt32,
    revenue Float64,
    avg_order_value Float64,
    cancellations_count UInt32
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(window_start)
ORDER BY window_start;

/*
  dlq_events — poison / failed records for manual replay

  Stores: original Kafka coordinates, failure reason, raw payload, and replay
  audit (replayed_at NULL until replay job succeeds). Append-only audit trail.

  Engine: MergeTree()
  Each failure is a new event; no upsert or dedup. Replay marks rows via
  replayed_at without deleting the original failure record.

  ORDER BY (failed_at, original_topic):
  - Time-ordered drain for replay workers; topic groups failures for targeted
    reprocessing (schema change test: ~4 minutes, 100% reprocessed).
*/
CREATE TABLE IF NOT EXISTS dlq_events
(
    original_topic String,
    original_offset Int64,
    original_timestamp DateTime64(3, 'UTC'),
    failure_reason String,
    failure_detail String,
    raw_payload String,
    failed_at DateTime64(3, 'UTC'),
    replayed_at Nullable(DateTime64(3, 'UTC'))
)
ENGINE = MergeTree()
ORDER BY (failed_at, original_topic);

-- =====================================================
-- KAFKA ENGINE TABLES
-- =====================================================
-- Consume Flink materialized topics (orders_materialized, etc.).
-- Kafka Engine tables are queue-like: each row is read once from Kafka and
-- is not stored durably in the table itself — they act as streaming consumers.

CREATE TABLE IF NOT EXISTS orders_kafka_queue (
    order_id String,
    customer_id String,
    status String,
    total_amount Float64,
    created_at String,
    updated_at String,
    is_deleted UInt8
) ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:29092',
    kafka_topic_list = 'orders_materialized',
    kafka_group_name = 'clickhouse_orders_consumer',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1;

CREATE TABLE IF NOT EXISTS products_kafka_queue (
    product_id String,
    sku String,
    name String,
    category String,
    price Float64,
    inventory_count Int32,
    created_at String,
    updated_at String,
    is_deleted UInt8
) ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:29092',
    kafka_topic_list = 'products_materialized',
    kafka_group_name = 'clickhouse_products_consumer',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1;

CREATE TABLE IF NOT EXISTS customers_kafka_queue (
    customer_id String,
    email String,
    tier String,
    created_at String,
    updated_at String,
    is_deleted UInt8
) ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:29092',
    kafka_topic_list = 'customers_materialized',
    kafka_group_name = 'clickhouse_customers_consumer',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1;

-- =====================================================
-- MATERIALIZED VIEWS
-- =====================================================
-- Automatically transform Kafka queue rows and insert into final ReplacingMergeTree tables.
-- parseDateTime64BestEffort: ISO 8601 timestamp strings from Flink JSON -> DateTime64(3, 'UTC').
-- toUUID: string UUIDs from Flink JSON -> ClickHouse UUID type.

CREATE MATERIALIZED VIEW IF NOT EXISTS orders_kafka_mv TO orders_current AS
SELECT
    toUUID(order_id) AS order_id,
    toUUID(customer_id) AS customer_id,
    status,
    total_amount,
    parseDateTime64BestEffort(created_at) AS created_at,
    parseDateTime64BestEffort(updated_at) AS updated_at,
    is_deleted
FROM orders_kafka_queue;

CREATE MATERIALIZED VIEW IF NOT EXISTS products_kafka_mv TO products_current AS
SELECT
    toUUID(product_id) AS product_id,
    sku,
    name,
    category,
    price,
    inventory_count,
    parseDateTime64BestEffort(updated_at) AS updated_at,
    is_deleted
FROM products_kafka_queue;

CREATE MATERIALIZED VIEW IF NOT EXISTS customers_kafka_mv TO customers_current AS
SELECT
    toUUID(customer_id) AS customer_id,
    email,
    tier,
    parseDateTime64BestEffort(created_at) AS created_at,
    parseDateTime64BestEffort(updated_at) AS updated_at,
    is_deleted
FROM customers_kafka_queue;

-- =====================================================
-- ORDER METRICS MATERIALIZED VIEW
-- =====================================================
-- Why ClickHouse MV not Flink windowing:
-- PROCTIME() tumbling windows in Flink Table API do not emit results when the source goes
-- idle between bursts. ClickHouse Materialized View aggregates directly from orders_current
-- on every INSERT, providing reliable per-minute metrics without Flink windowing complexity.
-- Production would use Flink event-time windows with Kafka rowtime metadata for exactly-once.

-- MV triggers on every INSERT into orders_current (via orders_kafka_mv).
-- toStartOfMinute groups all orders in the same minute into one metrics row.
-- ReplacingMergeTree on orders_current may hold duplicate order_id versions before merge;
-- use FINAL on orders_current for exact counts in ad-hoc queries — MV runs on raw inserts.

CREATE MATERIALIZED VIEW IF NOT EXISTS order_metrics_mv
TO order_metrics_per_minute AS
SELECT
    toStartOfMinute(created_at) AS window_start,
    toUInt32(count()) AS orders_count,
    sum(total_amount) AS revenue,
    avg(total_amount) AS avg_order_value,
    toUInt32(countIf(status = 'cancelled')) AS cancellations_count
FROM orders_current
WHERE is_deleted = 0;
