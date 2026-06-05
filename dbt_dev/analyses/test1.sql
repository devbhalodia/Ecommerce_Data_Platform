select * from
{{ ref("stg_silver__order_items") }} e1
join
{{ ref("stg_silver__payments") }} e2
on e1.order_id = e2.order_id
