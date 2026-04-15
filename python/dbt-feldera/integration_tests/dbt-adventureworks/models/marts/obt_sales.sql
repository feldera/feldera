{#
    One Big Table (OBT) - Denormalized flat table joining fact + all dimensions.
#}
with f_sales as (
    select * from {{ ref('fct_sales') }}
),

d_customer as (
    select * from {{ ref('dim_customer') }}
),

d_credit_card as (
    select * from {{ ref('dim_credit_card') }}
),

d_address as (
    select * from {{ ref('dim_address') }}
),

d_order_status as (
    select * from {{ ref('dim_order_status') }}
),

d_product as (
    select * from {{ ref('dim_product') }}
),

d_date as (
    select * from {{ ref('dim_date') }}
)

select
    -- Fact columns
    f_sales.sales_key,
    f_sales.salesorderid,
    f_sales.salesorderdetailid,
    f_sales.unitprice,
    f_sales.orderqty,
    f_sales.revenue,
    -- Product dimension
    d_product.productid,
    d_product.product_name,
    d_product.productnumber,
    d_product.color,
    d_product.class,
    d_product.product_subcategory_name,
    d_product.product_category_name,
    -- Customer dimension
    d_customer.customerid,
    d_customer.businessentityid,
    d_customer.fullname,
    d_customer.storebusinessentityid,
    d_customer.storename,
    -- Credit card dimension
    d_credit_card.creditcardid,
    d_credit_card.cardtype,
    -- Address dimension
    d_address.addressid,
    d_address.city_name,
    d_address.state_name,
    d_address.country_name,
    -- Order status dimension
    d_order_status.order_status,
    d_order_status.order_status_name,
    -- Date dimension
    d_date.date_key as order_date_key
from f_sales
left join d_product on f_sales.product_key = d_product.product_key
left join d_customer on f_sales.customer_key = d_customer.customer_key
left join d_credit_card on f_sales.creditcard_key = d_credit_card.creditcard_key
left join d_address on f_sales.ship_address_key = d_address.address_key
left join d_order_status on f_sales.order_status_key = d_order_status.order_status_key
left join d_date on f_sales.order_date_key = d_date.date_key
