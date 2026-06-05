with cte as (
    select distinct address_id, country, state, city, postal_code
    from {{ ref("int_sales_events") }}
)

select *,
row_number() over (order by address_id asc) as location_sk
from cte