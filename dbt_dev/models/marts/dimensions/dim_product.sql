with cte as (
    select distinct product_id, is_product_active, category, product_name
    from {{ ref("int_sales_events") }}
)

select *,
row_number() over (order by product_id asc) as product_sk
from cte