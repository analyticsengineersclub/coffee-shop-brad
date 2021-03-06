{{config(
    materialized='table'
) }}
select c.id as customer_id
, c.name
, c.email 
, min(created_at) as first_order_at
, count(distinct o.id) as num_orders
from {{ source('coffee_shop', 'customers') }} c
join  {{ source('coffee_shop', 'orders') }} o
on c.id = o.customer_id
group by 1,2,3
order by 4 