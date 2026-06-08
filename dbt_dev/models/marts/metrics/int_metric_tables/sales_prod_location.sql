select e1.sales_amount, e2.category, e3.city, e3.state, e3.country 
from {{ ref("fact_sales") }} e1
join {{ ref("dim_product") }} e2
on e1.product_sk = e2.product_sk
join {{ ref("dim_location") }} e3
on e1.location_sk = e3.location_sk