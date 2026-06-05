with cte1 as (
    select 
        e1.order_item_id, 
        e1.quantity, 
        e1.item_price, 
        e1.product_id, 
        e2.order_id, 
        e2.order_timestamp, 
        e2.order_status, 
        e2.user_id 
    from {{ ref("stg_silver__order_items") }} e1
    join {{ ref("stg_silver__orders") }} e2
    on e1.order_id = e2.order_id
)

, cte2 as (
    select 
        e1.order_item_id, 
        e1.quantity, 
        e1.item_price, 
        e1.product_id, 
        e1.order_id, 
        e1.order_timestamp, 
        e1.order_status, 
        e1.user_id,
        e2.cost, 
        e2.is_active as is_product_active, 
        e2.category, 
        e2.product_name
    from cte1 e1
    join {{ ref("stg_silver__products") }} e2
    on e1.product_id = e2.product_id
)

, cte3 as (
    select
        e1.order_item_id,
        e1.quantity,
        e1.item_price,
        e1.product_id,
        e1.order_id,
        e1.order_timestamp,
        e1.order_status,
        e1.user_id,
        e1.cost,
        e1.is_product_active,
        e1.category,
        e1.product_name,
        e2.is_active as is_user_active,
        e2.phone,
        e2.first_name,
        e2.last_name,
        e2.email
    from cte2 e1
    join {{ ref("stg_silver__users") }} e2
        on e1.user_id = e2.user_id
)

,  cte4 as (
    select 
    e1.order_item_id, 
    e1.quantity, 
    e1.item_price, 
    e1.product_id, 
    e1.order_id, 
    e1.order_timestamp, 
    e1.order_status, 
    e1.user_id,
    e1.cost, 
    e1.is_product_active, 
    e1.category, 
    e1.product_name,
    e1.is_user_active, 
    e1.phone, 
    e1.first_name, 
    e1.last_name, 
    e1.email,
    e2.address_id, 
    e2.country, 
    e2.state, 
    e2.city, 
    e2.postal_code
    from cte3 e1
    left join {{ ref("stg_silver__addresses") }} e2
    on e1.user_id = e2.user_id
)

select * from cte4