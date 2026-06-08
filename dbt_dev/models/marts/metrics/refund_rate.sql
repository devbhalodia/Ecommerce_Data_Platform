select round(count(distinct(e2.order_id))*100.0/count(distinct(e1.order_id)),2) as refund_rate
from {{ ref("fact_sales") }} e1
left join {{ ref("fact_refunds") }} e2
on e1.order_id = e2.order_id