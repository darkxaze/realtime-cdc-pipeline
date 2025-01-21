{{
  config(
    materialized='incremental',
    unique_key='order_id',
    incremental_strategy='delete+insert',
    on_schema_change='fail'
  )
}}

/*
  materialized=incremental
    Only rows with updated_at newer than the staging table watermark are read
    on each run. Full refresh remains available via dbt run --full-refresh.

  unique_key=order_id
    One analytics row per order. CDC emits a new physical row on every status
    change; the key tells dbt which logical entity each batch row belongs to.

  incremental_strategy=delete+insert
    Rejected: merge / in-place UPDATE — ClickHouse has no row-level UPDATE on
    MergeTree family tables. delete+insert removes stale keys for the batch
    then inserts the latest snapshot, matching ReplacingMergeTree upsert semantics.

  on_schema_change=fail
    Rejected: sync_all_columns — silent column drift hides Flink/Debezium schema
    evolution bugs. Fail loudly so we fix upstream before gold models break.
*/

select
    order_id,
    customer_id,
    lower(status) as status,
    total_amount,
    created_at,
    updated_at,
    case
        when created_at >= '{{ var("flash_sale_start") }}'
            and created_at <= '{{ var("flash_sale_end") }}'
        then 1
        else 0
    end as is_flash_sale_order,
    dateDiff('second', created_at, updated_at) as seconds_in_current_status,
    is_deleted
from {{ source('default', 'orders_current') }} final
where is_deleted = 0
{% if is_incremental() %}
    and updated_at > (select max(updated_at) from {{ this }})
{% endif %}
