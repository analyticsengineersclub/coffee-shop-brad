{% set prod_types = ['coffee beans', 'merch', 'brewing supplies'] %}

select
  date_trunc(order_created_at, month) as date_month,
  {% for prod_type in prod_types %}
    sum(case when product_category = '{{ prod_type }}' then product_price end) as {{prod_type | replace(' ', '_') }}
  {% if not loop.last %},{% endif %}
  {% endfor %}
from {{ ref('order_items') }}
group by 1