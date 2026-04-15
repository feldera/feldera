with stg_order_status as (
    select distinct status as order_status
    from
        {{ ref('salesorderheader') }}
)

select
    {{ generate_surrogate_key(['stg_order_status.order_status']) }} as order_status_key,
    order_status,
    case
        when order_status = 1 then 'in_process'
        when order_status = 2 then 'approved'
        when order_status = 3 then 'backordered'
        when order_status = 4 then 'rejected'
        when order_status = 5 then 'shipped'
        when order_status = 6 then 'cancelled'
        else 'no_status'
    end as order_status_name
from stg_order_status
