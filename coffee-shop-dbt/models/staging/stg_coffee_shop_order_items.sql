

select o.order_id
    , o.order_created_at
    , p.product_id
    , p.product_name
    , p.product_category 
    , pp.product_price 
from {{ ref('stg_coffee_shop_orders') }} o
join {{ source('coffee_shop','order_items')}} oi 
    on o.order_id = oi.order_id 
join {{ ref('stg_coffee_shop_products') }} as p 
    on oi.product_id = p.product_id 
join {{ ref('stg_coffee_shop_product_prices') }} pp 
    on oi.product_id = pp.product_id
  and o.order_created_at between pp.product_price_started_at and pp.product_price_ended_at


