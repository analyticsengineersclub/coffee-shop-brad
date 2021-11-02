select distinct visitor_id 
, customer_id 
from {{ ref('stg_pageviews')}}
where customer_id is not null