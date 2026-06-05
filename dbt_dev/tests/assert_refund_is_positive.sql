select * from {{ ref("stg_silver__refunds") }}
where refund_amount <= 0