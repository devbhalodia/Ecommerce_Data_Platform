with cte as (select order_id, sum(quantity*item_price) as total_amount, count(*) as cnt
from  {{ ref("stg_silver__order_items") }}
group by order_id
)

select * from cte e1
join {{ ref("stg_silver__payments") }} e2
on e1.order_id = e2.order_id
where e1.total_amount < e2.amount or e1.total_amount > e2.amount