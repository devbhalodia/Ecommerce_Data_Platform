select category, sum(sales_amount) as revenue from {{ ref("sales_prod_location") }}
group by category