WITH customer_weeks AS (
    SELECT 
        co.customer_id
        , dd.week_starting as week_starting
        , case when date(dd.day) = date(order_created_at) 
            THEN order_id else null end 
        as order_id
    FROM {{ ref('dim_date')}} as dd
    CROSS JOIN {{ ref('customer_orders')}} as co 
    WHERE dd.day >= date(order_created_at)
    GROUP BY 1,2,3
)

, all_orders as ( 
    SELECT 
        cw.customer_id
        , cw.week_starting
        , count(distinct co.order_id) as order_count
        , sum(co.order_total) as week_revenue
    FROM customer_weeks cw 
    LEFT JOIN  {{ ref('customer_orders')}} co 
        ON co.customer_id = cw.customer_id
        and co.order_id = cw.order_id
    group by 1,2
)

SELECT 
    customer_id
    , week_starting
    , coalesce(week_revenue,0) as week_revenue
    , order_count
    , min(week_starting) over(partition by customer_id) as cohort_date_start 
    , row_number() over(partition by customer_id order by week_starting) as week_num
    , sum(order_count) over(partition by customer_id order by week_starting) as cumulative_order_count
    , sum(week_revenue) over(partition by customer_id order by week_starting) as cumulative_revenue 
FROM all_orders 