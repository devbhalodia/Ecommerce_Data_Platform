select * from
 {{ ref('stg_silver__order_items')}}
 where quantity <= 0 or item_price <= 0