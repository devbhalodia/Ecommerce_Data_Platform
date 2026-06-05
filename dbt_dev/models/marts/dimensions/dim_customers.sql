with cte as (
    select distinct user_id, is_user_active, phone, first_name, last_name, email
    from {{ ref("int_sales_events") }}
)

select *,
row_number() over (order by user_id asc) as customer_sk
from cte