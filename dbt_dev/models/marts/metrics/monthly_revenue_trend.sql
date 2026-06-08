select year, month, sum(sales_amount) as revenue from {{ ref("fact_sales") }} e1
join {{ ref("dim_date") }} e2
on e1.date_sk = e2.date_sk
group by year, month