with cte as (
    select e1.refund_id, e1.refund_amount, e1.refund_reason, e1.refund_timestamp, e1.order_id, e2.user_id from {{ ref("stg_silver__refunds") }} e1
    join {{ ref("stg_silver__orders") }} e2
    on e1.order_id = e2.order_id
)

select e1.refund_id, e1.refund_amount, e1.refund_reason, e1.refund_timestamp, e1.order_id, e2.customer_sk, e3.date_sk
from cte e1
join {{ ref("dim_customers") }} e2
on e1.user_id = e2.user_id
join {{ ref("dim_date") }} e3
on cast(e1.refund_timestamp as date) = e3.full_date