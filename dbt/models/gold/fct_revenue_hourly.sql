{{
  config(
    materialized='table',
    schema='gold'
  )
}}

select
    toStartOfHour(window_start) as hour,
    sum(orders_count) as orders_count,
    sum(revenue) as revenue,
    avg(avg_order_value) as avg_order_value,
    sum(cancellations_count) as cancellations_count,
    sum(cancellations_count) / sum(orders_count) as cancellation_rate
from {{ source('default', 'order_metrics_per_minute') }}
group by toStartOfHour(window_start)
order by hour
