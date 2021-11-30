select 
    pageviews.pageview_id 
    , pageviews.visitor_id 
    , pageviews.device_type
    , pageviews.timestamp
    , pageviews.pagename 
    , vc_map.customer_id
from {{ ref('stg_pageviews') }} as pageviews 
left join {{ ref('stg_vistor_customer_map') }} as vc_map
    on pageviews.visitor_id = vc_map.visitor_id