select c.customer_id
    , c.name
    , c.email
    , o.order_id
    , o.order_total
    , o.address 
    , o.state 
    , o.zip 
    , o.order_created_at
    , co.first_order_at
    , case 
        when order_created_at = first_order_at then TRUE
        when order_created_at > first_order_at then FALSE
        else null 
      end as is_new_customer
    , row_number() over(partition by c.customer_id order by o.order_created_at) as order_number
from {{ ref('stg_coffee_shop_customers') }} as c
join {{ ref('stg_coffee_shop_orders') }} as o 
    on c.customer_id = o.customer_id
join {{ ref('stg_coffee_shop_customer_orders') }} as co 
    on c.customer_id = co.customer_id