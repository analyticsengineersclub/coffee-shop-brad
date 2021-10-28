{{config(
    materialized='view'
) }}

with orders as (
    select customer_id
        , min(created_at) as first_order_at 
        , count(*) as num_orders
        , sum(total) as lifetime_revenue 
    from {{ source('coffee_shop','orders')}}
    group by 1
)
select c.customer_id
, c.name
, c.email 
, o.first_order_at
, o.num_orders
, o.lifetime_revenue
from {{ ref('stg_coffee_shop_customers') }} c
left join  orders o 
    on c.customer_id = o.customer_id
