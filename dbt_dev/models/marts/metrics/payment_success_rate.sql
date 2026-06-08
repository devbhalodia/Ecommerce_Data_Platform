select round(count(case when payment_status = 'success' then 1 else 0 end)*100/count(*),2) as payment_success_rate 
from {{ ref("fact_payments") }}