select 
    sessions.visitor_id
    , sessions.visitor_session_id
    , SUM(CASE WHEN 
        sessions.is_new_session = 1
        THEN date_diff(sessions.session_end_at, sessions.session_start_at,second) 
        ELSE 0 
    END) AS session_length
    , count(*) as pageviews
    , count(distinct pagename) as unique_pageviews
    , SUM(CASE WHEN 
        pagename = 'order-confirmation' 
        THEN 1 ELSE 0 END) AS num_orders
FROM {{ ref('sessions')}} as sessions
GROUP BY 1,2 