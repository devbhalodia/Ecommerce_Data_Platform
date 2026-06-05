select * from {{ ref("stg_silver__payments") }} e1
join
{{ ref("stg_silver__refunds") }} e2
on e1.order_id = e2.order_id
where amount > refund_amount OR amount < refund_amount