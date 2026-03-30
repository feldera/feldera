with stg_salesorderheader as (
    select
        salesorderid,
        customerid,
        creditcardid,
        shiptoaddressid,
        status as order_status,
        cast(orderdate as date) as orderdate
    from {{ ref('salesorderheader') }}
),

stg_salesorderdetail as (
    select
        salesorderid,
        salesorderdetailid,
        productid,
        orderqty,
        unitprice,
        unitprice * orderqty as revenue
    from {{ ref('salesorderdetail') }}
),

stg_kafka_sales as (
    select
        salesorderid,
        salesorderdetailid,
        productid,
        customerid,
        creditcardid,
        shiptoaddressid,
        order_status,
        cast(orderdate as date) as orderdate,
        orderqty,
        unitprice,
        unitprice * orderqty as revenue
    from {{ ref('kafka_sales') }}
)

select
    {{ generate_surrogate_key(['stg_salesorderdetail.salesorderid', 'salesorderdetailid']) }} as sales_key,
    {{ generate_surrogate_key(['productid']) }} as product_key,
    {{ generate_surrogate_key(['customerid']) }} as customer_key,
    {{ generate_surrogate_key(['creditcardid']) }} as creditcard_key,
    {{ generate_surrogate_key(['shiptoaddressid']) }} as ship_address_key,
    {{ generate_surrogate_key(['order_status']) }} as order_status_key,
    {{ generate_surrogate_key(['orderdate']) }} as order_date_key,
    stg_salesorderdetail.salesorderid,
    stg_salesorderdetail.salesorderdetailid,
    stg_salesorderdetail.unitprice,
    stg_salesorderdetail.orderqty,
    stg_salesorderdetail.revenue
from stg_salesorderdetail
inner join stg_salesorderheader on stg_salesorderdetail.salesorderid = stg_salesorderheader.salesorderid

UNION ALL

select
    {{ generate_surrogate_key(['stg_kafka_sales.salesorderid', 'stg_kafka_sales.salesorderdetailid']) }} as sales_key,
    {{ generate_surrogate_key(['stg_kafka_sales.productid']) }} as product_key,
    {{ generate_surrogate_key(['stg_kafka_sales.customerid']) }} as customer_key,
    {{ generate_surrogate_key(['stg_kafka_sales.creditcardid']) }} as creditcard_key,
    {{ generate_surrogate_key(['stg_kafka_sales.shiptoaddressid']) }} as ship_address_key,
    {{ generate_surrogate_key(['stg_kafka_sales.order_status']) }} as order_status_key,
    {{ generate_surrogate_key(['stg_kafka_sales.orderdate']) }} as order_date_key,
    stg_kafka_sales.salesorderid,
    stg_kafka_sales.salesorderdetailid,
    stg_kafka_sales.unitprice,
    stg_kafka_sales.orderqty,
    stg_kafka_sales.revenue
from stg_kafka_sales
