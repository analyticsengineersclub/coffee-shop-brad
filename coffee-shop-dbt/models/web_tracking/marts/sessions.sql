select 
    pageviews.pageview_id 
    , pageviews.visitor_id 
    , pageviews.device_type
    , pageviews.timestamp
    , pageviews.pagename 
    , pageviews.customer_id
    , sessions.visitor_session_id 
    , sessions.session_start_at
    , MAX(pageviews.timestamp) over(partition by pageviews.visitor_id, sessions.visitor_session_id) as session_end_at
    , CASE WHEN 
        row_number() over(partition by pageviews.visitor_id, sessions.visitor_session_id order by pageviews.timestamp) = 1 
        THEN 1 ELSE 0 END AS is_new_session
FROM {{ ref('pageviews') }} as pageviews 
LEFT JOIN {{ ref('stg_sessions')}} as sessions
    ON pageviews.visitor_id = sessions.visitor_id
    AND pageviews.timestamp >= sessions.session_start_at
    AND (pageviews.timestamp < sessions.next_session_start_at OR sessions.next_session_start_at is null)