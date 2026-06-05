with sales as (
    select * from {{ ref("int_sales_events") }}
)

select 
e2.customer_sk,
e3.product_sk,
e4.location_sk,
e5.date_sk,
e1.order_item_id,
e1.order_id,
e1.order_status,
e1.quantity,
e1.item_price,
e1.quantity*e1.item_price as sales_amount,
e1.quantity*e1.item_price - e1.quantity*e1.cost as profit_amount
from sales e1
join {{ ref("dim_customers") }} e2
on e1.user_id = e2.user_id
join {{ ref("dim_product") }} e3
on e1.product_id = e3.product_id
join {{ ref("dim_location") }} e4
on e1.address_id = e4.address_id
join {{ ref("dim_date") }} e5
on cast(e1.order_timestamp as date) = e5.full_date