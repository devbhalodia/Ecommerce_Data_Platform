
with cte as (
    select e1.payment_id, e1.payment_timestamp, e1.amount, e1.payment_status, e1.order_id, e1.payment_method, e2.user_id from {{ ref("stg_silver__payments") }} e1
    join {{ ref("stg_silver__orders") }} e2
    on e1.order_id = e2.order_id
)

select e1.payment_id, e1.payment_timestamp, e1.amount, e1.payment_status, e1.order_id, e1.payment_method, e2.customer_sk, e3.date_sk from cte e1
join {{ ref("dim_customers") }} e2
on e1.user_id = e2.user_id
join {{ ref("dim_date") }} e3
on cast(e1.payment_timestamp as date) = e3.full_date