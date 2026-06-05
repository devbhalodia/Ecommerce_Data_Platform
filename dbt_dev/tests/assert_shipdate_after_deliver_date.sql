select * from {{ ref("stg_silver__shipments") }}
where shipped_date is not null and delivered_date is not null and shipped_date > delivered_date