{{
  config(
    materialized='table',
    schema='gold'
  )
}}

/*
  This model is only possible because of CDC.
  Batch ETL sees only final status. CDC captures every status change.
  This model measures time-in-status which batch analytics cannot produce.
*/

-- FINAL is applied inside a subquery because ClickHouse does not
-- support FINAL on JOIN clauses, only on the main FROM table.
-- The subquery deduplicates customer rows at query time.

SELECT
    o.order_id,
    o.customer_id,
    c.tier AS customer_tier,
    o.total_amount,
    o.status AS current_status,
    o.created_at,
    o.updated_at,
    o.is_flash_sale_order,
    o.seconds_in_current_status,
    CASE
        WHEN o.status = 'confirmed'
        THEN o.seconds_in_current_status
        ELSE NULL
    END AS seconds_to_confirm,
    CASE
        WHEN o.is_flash_sale_order = 1 THEN 'flash_sale'
        ELSE 'normal'
    END AS order_context
FROM {{ ref('stg_orders') }} o
LEFT JOIN (
    SELECT customer_id, tier
    FROM {{ source('default', 'customers_current') }} FINAL
    WHERE is_deleted = 0
) c ON o.customer_id = c.customer_id
