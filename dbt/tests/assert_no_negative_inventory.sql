-- Defence in depth: Postgres enforces inventory_count >= 0 via CHECK constraint,
-- but CDC/Flink/ClickHouse could still surface corrupt rows; this singular test
-- fails loudly if negative inventory appears in the analytics layer.

select
    product_id,
    sku,
    inventory_count
from {{ ref('stg_products') }}
where inventory_count < 0
