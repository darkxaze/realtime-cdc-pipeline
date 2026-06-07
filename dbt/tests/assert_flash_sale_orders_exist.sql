-- Only meaningful after running the flash sale load generator. Fails on a fresh
-- database with no flash sale data — expected and correct behaviour.

select 1
from {{ ref('mart_flash_sale_analysis') }}
where flash_sale_order_count = 0
