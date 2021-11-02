select o.order_created_at 
    , o.order_id
    , o.customer_id
    , oi.product_id
    , oi.product_name 
    , oi.product_category 
    , oi.product_price 
from {{ ref('stg_coffee_shop_orders') }} o 
join {{ ref('stg_coffee_shop_order_items') }} oi 
    on o.order_id = oi.order_id 