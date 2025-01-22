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

select
    o.order_id,
    o.customer_id,
    c.tier as customer_tier,
    o.total_amount,
    o.status as current_status,
    o.created_at,
    o.updated_at,
    o.is_flash_sale_order,
    o.seconds_in_current_status,
    case
        when o.status = 'confirmed'
        then o.seconds_in_current_status
        else null
    end as seconds_to_confirm,
    case
        when o.is_flash_sale_order = 1 then 'flash_sale'
        else 'normal'
    end as order_context
from {{ ref('stg_orders') }} o
left join {{ source('default', 'customers_current') }} c
    on o.customer_id = c.customer_id
    and c.is_deleted = 0
