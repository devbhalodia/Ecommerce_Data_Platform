select sum(sales_amount) as total_revenue,
round(sum(profit_amount)*100.0/nullif(sum(sales_amount), 0),2) as profit_margin_pct
from {{ ref("fact_sales") }}