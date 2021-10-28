with source as (
    select * from {{ source('coffee_shop', 'orders')}}
),

renamed as (
    select id as order_id
        , customer_id 
        , total as order_total
        , address
        , state
        , zip
        , created_at as order_created_at
    from source
)

select * from renamed