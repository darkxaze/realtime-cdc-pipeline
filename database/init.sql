-- Stage 1 source schema for e-commerce CDC demos.
-- gen_random_uuid() is built-in from PostgreSQL 13 onward (image uses 15).

-- Customers: master entity for who buys; CDC carries inserts (signup) and updates
-- (e.g. tier changes) so downstream analytics can correct customer attributes without
-- a full snapshot reload.
CREATE TABLE customers (
  customer_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  email VARCHAR(255) UNIQUE NOT NULL,
  tier VARCHAR(20) DEFAULT 'standard'
    CONSTRAINT customers_tier_check CHECK (tier IN ('standard', 'premium', 'vip')),
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Products: catalog and stock; CDC must surface price and inventory_count changes
-- (especially under load) so the warehouse reflects intermediate updates, not only
-- the latest batch snapshot.
CREATE TABLE products (
  product_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  sku VARCHAR(50) UNIQUE NOT NULL,
  name VARCHAR(255) NOT NULL,
  category VARCHAR(100),
  price DECIMAL(10,2) CONSTRAINT products_price_positive CHECK (price > 0),
  inventory_count INT DEFAULT 0
    CONSTRAINT products_inventory_nonnegative CHECK (inventory_count >= 0),
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Orders: transactional header; status transitions generate update events that
-- test whether dashboards stay fresh (vs stale “pending” rows) when the pipeline
-- processes the WAL in order.
CREATE TABLE orders (
  order_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  customer_id UUID NOT NULL
    REFERENCES customers (customer_id) ON DELETE RESTRICT,
  status VARCHAR(50) DEFAULT 'pending'
    CONSTRAINT orders_status_check CHECK (
      status IN ('pending', 'confirmed', 'shipped', 'delivered', 'cancelled')
    ),
  total_amount DECIMAL(10,2) CONSTRAINT orders_total_positive CHECK (total_amount > 0),
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Order items: line-level facts tied to orders and products; CDC volume scales with
-- basket size and is used to validate end-to-end fan-out (one order, many lines)
-- and referential integrity in the sink.
CREATE TABLE order_items (
  item_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  order_id UUID NOT NULL
    REFERENCES orders (order_id) ON DELETE RESTRICT,
  product_id UUID NOT NULL
    REFERENCES products (product_id) ON DELETE RESTRICT,
  quantity INT NOT NULL CONSTRAINT order_items_qty_positive CHECK (quantity > 0),
  unit_price DECIMAL(10,2) NOT NULL
    CONSTRAINT order_items_unit_price_positive CHECK (unit_price > 0)
);

-- FK lookups and status-driven load generator queries.
CREATE INDEX idx_orders_customer_id ON orders (customer_id);
CREATE INDEX idx_orders_status_open ON orders (status)
  WHERE status IN ('pending', 'confirmed', 'shipped');

-- Keep updated_at aligned with row mutations for tables that participate in
-- “when did this row last change?” checks and benchmarks.
CREATE OR REPLACE FUNCTION update_updated_at()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
  NEW.updated_at := NOW();
  RETURN NEW;
END;
$$;

CREATE TRIGGER customers_set_updated_at
  BEFORE UPDATE ON customers
  FOR EACH ROW
  EXECUTE FUNCTION update_updated_at();

CREATE TRIGGER products_set_updated_at
  BEFORE UPDATE ON products
  FOR EACH ROW
  EXECUTE FUNCTION update_updated_at();

CREATE TRIGGER orders_set_updated_at
  BEFORE UPDATE ON orders
  FOR EACH ROW
  EXECUTE FUNCTION update_updated_at();

/*
  REPLICA IDENTITY FULL: WAL before-images list every column, not only the primary key.
  CDC: Debezium needs those full before-rows to emit correct UPDATE payloads (old values).
  Cost: modestly larger WAL vs DEFAULT (key-only before-images).
*/
ALTER TABLE customers REPLICA IDENTITY FULL;
ALTER TABLE products REPLICA IDENTITY FULL;
ALTER TABLE orders REPLICA IDENTITY FULL;
ALTER TABLE order_items REPLICA IDENTITY FULL;

/*
  Publications: Postgres logical-replication metadata naming which tables replicate.
  Debezium: the connector’s replication slot consumes this publication’s changes.
  Alternative: publication.autocreate.mode in the connector; explicit CREATE stays obvious in git.
*/
CREATE PUBLICATION dbz_publication FOR ALL TABLES;

/*
  Why updated_at triggers matter for CDC latency measurement

  Latency is often measured from the moment a business row “changes meaningfully”
  until that change is visible in Kafka or the analytics store. If applications
  forget to bump updated_at on UPDATE, wall-clock comparisons and row-level
  “freshness” queries lie: the row changed in Postgres but your observable
  timestamp did not. BEFORE UPDATE triggers make updated_at consistent for every
  mutation that hits the table, so you can correlate WAL commit order, Debezium
  event time, and dashboard queries without depending on each caller to set the
  column correctly. That reduces false noise when you tune heartbeat, slot, or
  consumer lag and when you assert p50/p95 source-to-sink delay in benchmarks.
*/
