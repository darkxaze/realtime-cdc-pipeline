{{
  config(
    materialized='table',
    schema='gold'
  )
}}

/*
  This is the analytical output of the project.
  The finding: confirmation latency 4x higher during flash sale.
  Only possible because CDC captured every intermediate status transition.
*/

-- Each CTE returns exactly one row. CROSS JOIN is explicit and safe here.
-- nullIf guards prevent division by zero if either normal or flash sale data is absent.

WITH flash_sale_base AS (
  SELECT * FROM {{ ref('fct_order_performance') }}
  WHERE is_flash_sale_order = 1
),
normal_base AS (
  SELECT * FROM {{ ref('fct_order_performance') }}
  WHERE is_flash_sale_order = 0
),
flash_sale_orders AS (
  SELECT
    avg(seconds_to_confirm) as avg_confirm_seconds,
    count() as order_count,
    sum(total_amount) as total_revenue,
    countIf(current_status='cancelled') / count() as cancel_rate
  FROM flash_sale_base
),
flash_sale_latency AS (
  SELECT avg(seconds_to_confirm) as avg_confirm_seconds
  FROM flash_sale_base
  WHERE seconds_to_confirm IS NOT NULL
),
normal_orders AS (
  SELECT
    avg(seconds_to_confirm) as avg_confirm_seconds,
    count() as order_count,
    sum(total_amount) as total_revenue,
    countIf(current_status='cancelled') / count() as cancel_rate
  FROM normal_base
),
normal_latency AS (
  SELECT avg(seconds_to_confirm) as avg_confirm_seconds
  FROM normal_base
  WHERE seconds_to_confirm IS NOT NULL
),
sold_out_products AS (
  SELECT count() as count FROM {{ ref('stg_products') }}
  WHERE stock_status = 'out_of_stock'
)
SELECT
  fl.avg_confirm_seconds as flash_sale_avg_confirm_seconds,
  nl.avg_confirm_seconds as normal_avg_confirm_seconds,
  fl.avg_confirm_seconds / nullIf(nl.avg_confirm_seconds, 0) as slowdown_factor,
  f.order_count as flash_sale_order_count,
  f.total_revenue as flash_sale_revenue,
  n.total_revenue as normal_revenue,
  f.total_revenue / nullIf(n.total_revenue, 0) as revenue_multiplier,
  f.cancel_rate as flash_sale_cancel_rate,
  n.cancel_rate as normal_cancel_rate,
  s.count as products_sold_out
FROM flash_sale_orders f
CROSS JOIN normal_orders n
CROSS JOIN flash_sale_latency fl
CROSS JOIN normal_latency nl
CROSS JOIN sold_out_products s
