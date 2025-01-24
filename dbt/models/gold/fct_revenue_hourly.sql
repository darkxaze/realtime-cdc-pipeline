{{
  config(
    materialized='table',
    schema='gold'
  )
}}

-- Pre-aggregated per-minute rows rolled up to hourly grain.
-- avg_order_value and cancellation_rate computed in outer query
-- to avoid ClickHouse 23.11 ILLEGAL_AGGREGATION error when
-- division expressions reference aggregate aliases in same SELECT.

SELECT
  hour,
  orders_count,
  revenue,
  revenue / nullIf(orders_count, 0)              AS avg_order_value,
  cancellations_count,
  cancellations_count / nullIf(orders_count, 0)  AS cancellation_rate
FROM (
  SELECT
    toStartOfHour(window_start)    AS hour,
    sum(orders_count)              AS orders_count,
    sum(revenue)                   AS revenue,
    sum(cancellations_count)       AS cancellations_count
  FROM {{ source('default', 'order_metrics_per_minute') }}
  GROUP BY toStartOfHour(window_start)
  ORDER BY toStartOfHour(window_start)
)
