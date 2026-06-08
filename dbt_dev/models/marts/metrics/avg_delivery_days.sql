select
    round(avg(datediff(e3.full_date, e2.full_date)),2) as avg_delivery_days
from {{ ref("fact_shipments") }} e1
join {{ ref("dim_date") }} e2
    on e1.shipped_sk = e2.date_sk
join {{ ref("dim_date") }} e3
    on e1.delivered_sk = e3.date_sk
where e2.full_date is not null