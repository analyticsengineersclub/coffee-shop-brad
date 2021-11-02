SELECT
      row_number() over(partition by event.visitor_id order by event.timestamp) as visitor_session_id
      , event.visitor_id
      , event.timestamp as session_start_at
      , lead(timestamp) over(partition by event.visitor_id order by event.timestamp) as next_session_start_at
    FROM {{ ref('stg_inactivity_time')}} as event
WHERE (
    event.inactivity_time_minutes > 30 
    OR event.inactivity_time_minutes is null
    )