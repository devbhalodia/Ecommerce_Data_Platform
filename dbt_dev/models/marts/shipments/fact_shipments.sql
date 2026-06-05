with cte as (
    select e1.shipment_id, e1.carrier, e1.shipped_date, e1.order_id, e1.shipment_status, e1.delivered_date, e2.user_id from {{ ref("stg_silver__shipments") }} e1
    join {{ ref("stg_silver__orders") }} e2
    on e1.order_id = e2.order_id
)

select 
e1.shipment_id, 
e1.carrier, 
e1.shipped_date, 
e1.order_id, 
e1.shipment_status, 
e1.delivered_date, 
e1.user_id, 
e2.customer_sk,
e3.date_sk as shipped_sk,
e4.date_sk as delivered_sk
from cte e1
join {{ ref('dim_customers') }} e2
on e1.user_id = e2.user_id
join {{ ref("dim_date") }} e3
on cast(e1.shipped_date as date) = e3.full_date
left join {{ ref("dim_date") }} e4
on cast(e1.delivered_date as date) = e4.full_date