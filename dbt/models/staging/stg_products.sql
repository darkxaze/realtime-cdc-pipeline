{{
  config(
    materialized='incremental',
    unique_key='product_id',
    incremental_strategy='delete+insert',
    on_schema_change='fail'
  )
}}

/*
  materialized=incremental
    Only rows with updated_at newer than the staging table watermark are read
    on each run. Full refresh remains available via dbt run --full-refresh.

  unique_key=product_id
    One analytics row per SKU line. Inventory decrements during flash sale emit
    frequent versions; the key scopes delete+insert to the affected products.

  incremental_strategy=delete+insert
    Rejected: merge / in-place UPDATE — ClickHouse has no row-level UPDATE on
    MergeTree family tables. delete+insert removes stale keys for the batch
    then inserts the latest snapshot, matching ReplacingMergeTree upsert semantics.

  on_schema_change=fail
    Rejected: sync_all_columns — silent column drift hides Flink/Debezium schema
    evolution bugs. Fail loudly so we fix upstream before gold models break.
*/

select
    product_id,
    sku,
    name,
    category,
    price,
    inventory_count,
    case
        when inventory_count = 0 then 'out_of_stock'
        when inventory_count <= {{ var('low_stock_threshold', 10) }} then 'low_stock'
        else 'in_stock'
    end as stock_status,
    updated_at,
    is_deleted
from {{ source('default', 'products_current') }} final
where is_deleted = 0
{% if is_incremental() %}
    and updated_at > (select max(updated_at) from {{ this }})
{% endif %}
