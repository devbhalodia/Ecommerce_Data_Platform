with bounds as (

    select
        min(cast(order_timestamp as date)) as start_date,
        add_months(current_date(), 60) as end_date
    from {{ ref('int_sales_events') }}

)

select
    cast(date_format(d, 'yyyyMMdd') as bigint) as date_sk,
    d as full_date,
    year(d) as year,
    quarter(d) as quarter,
    month(d) as month,
    weekofyear(d) as week,
    day(d) as day,
    dayofweek(d) as day_of_week,
    date_format(d, 'MMMM') as month_name,
    date_format(d, 'EEEE') as day_name
from bounds
lateral view explode(
    sequence(start_date, end_date, interval 1 day)
) s as d