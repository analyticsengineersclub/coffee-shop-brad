{{config(
    materialized='view'
) }}

select c.id as customer_id
, c.name
, c.email 
from {{ source('coffee_shop', 'customers') }} c

