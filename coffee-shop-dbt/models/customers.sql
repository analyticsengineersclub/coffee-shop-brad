
select c.id as customer_id
, c.name
, c.email 
, min(created_at) as first_order_at
, count(distinct o.id) as num_orders
from analytics-engineers-club.coffee_shop.customers c
join  analytics-engineers-club.coffee_shop.orders o
on c.id = o.customer_id
group by 1,2,3
order by 4 