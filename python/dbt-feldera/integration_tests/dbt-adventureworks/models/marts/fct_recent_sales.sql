with stg_salesorderheader as (
    select
        salesorderid,
        customerid,
        creditcardid,
        shiptoaddressid,
        status as order_status,
        orderdate
    from {{ ref('salesorderheader') }}
)

select
    {{ generate_surrogate_key(['salesorderid']) }} as sales_key,
    salesorderid,
    customerid,
    creditcardid,
    shiptoaddressid,
    order_status,
    orderdate
from stg_salesorderheader
