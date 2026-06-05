select * from
{{ ref("stg_silver__products")}}
where cost <= 0 or price <= 0 or price < cost