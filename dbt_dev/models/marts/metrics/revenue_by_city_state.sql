select city, state, country, sum(sales_amount) as revenue from {{ ref("sales_prod_location") }} 
group by country, state, city