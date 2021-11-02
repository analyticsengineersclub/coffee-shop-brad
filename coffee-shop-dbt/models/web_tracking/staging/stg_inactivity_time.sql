select 
    pageviews.pageview_id 
    , pageviews.visitor_id 
    , pageviews.timestamp
    , date_diff(timestamp, lag(timestamp) over (PARTITION BY visitor_id order by timestamp), second) / 60 as inactivity_time_minutes
from {{ref('stg_pageviews')}} as pageviews 