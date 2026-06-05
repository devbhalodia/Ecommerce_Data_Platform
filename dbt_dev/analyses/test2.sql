select e1.amount, e2.refund_amount from
{{ ref("stg_silver__payments") }} e1
join
{{ ref("stg_silver__refunds") }} e2
on e1.order_id = e2.order_id