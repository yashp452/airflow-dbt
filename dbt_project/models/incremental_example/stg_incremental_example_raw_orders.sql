with
    source as (
        select
            id
            , user_id
            , order_date
            , status
            , last_updated_dt
        from {{ source('bootcamp', 'incremental_example_raw_orders') }}
    )

select *
from source
{% if is_incremental() %}
    where last_updated_dt > (select max(last_updated_dt) from {{ this }})
{% endif %}