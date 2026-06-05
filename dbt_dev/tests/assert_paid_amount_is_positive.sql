select * from {{ ref("stg_silver__payments") }}
where amount < 0