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

with flash_sale_orders as (
    select
        avg(seconds_to_confirm) as avg_confirm_seconds,
        count() as order_count,
        sum(total_amount) as total_revenue,
        countIf(current_status = 'cancelled') / count() as cancel_rate
    from {{ ref('fct_order_performance') }}
    where is_flash_sale_order = 1
        and seconds_to_confirm is not null
),

normal_orders as (
    select
        avg(seconds_to_confirm) as avg_confirm_seconds,
        count() as order_count,
        sum(total_amount) as total_revenue,
        countIf(current_status = 'cancelled') / count() as cancel_rate
    from {{ ref('fct_order_performance') }}
    where is_flash_sale_order = 0
        and seconds_to_confirm is not null
),

sold_out_products as (
    select count() as count
    from {{ ref('stg_products') }}
    where stock_status = 'out_of_stock'
)

select
    f.avg_confirm_seconds as flash_sale_avg_confirm_seconds,
    n.avg_confirm_seconds as normal_avg_confirm_seconds,
    f.avg_confirm_seconds / n.avg_confirm_seconds as slowdown_factor,
    f.order_count as flash_sale_order_count,
    f.total_revenue as flash_sale_revenue,
    n.total_revenue as normal_revenue,
    f.total_revenue / n.total_revenue as revenue_multiplier,
    f.cancel_rate as flash_sale_cancel_rate,
    n.cancel_rate as normal_cancel_rate,
    s.count as products_sold_out
from flash_sale_orders f, normal_orders n, sold_out_products s
