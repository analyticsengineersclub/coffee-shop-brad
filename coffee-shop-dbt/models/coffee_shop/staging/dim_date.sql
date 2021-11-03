  SELECT day 
  , date_trunc(day,week) as week_starting
FROM UNNEST(
    GENERATE_DATE_ARRAY(DATE('2021-01-01'), CURRENT_DATE(), INTERVAL 1 DAY)
) AS day
order by 1