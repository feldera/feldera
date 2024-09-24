-- SQL program for the Feldera Snowflake sink demo
-- (Based on the tutorial: https://docs.feldera.com/tutorials/basics/)

create table VENDOR (
    id bigint not null primary key,
    name varchar,
    address varchar
);

create table PART (
    id bigint not null primary key,
    name varchar
);

create table PRICE (
    part bigint not null,
    vendor bigint not null,
    created timestamp,
    effective_since date,
    price decimal,
    f real
);

create view PRICE_OUT as select * from PRICE;

-- Lowest available price for each part across all vendors.
create view LOW_PRICE (
    part,
    price
) as
    select part, MIN(price) as price from PRICE group by part;

-- Lowest available price for each part along with part and vendor details.
create view PREFERRED_VENDOR (
    part_id,
    part_name,
    vendor_id,
    vendor_name,
    price
) as
    select
        PART.id as part_id,
        PART.name as part_name,
        VENDOR.id as vendor_id,
        VENDOR.name as vendor_name,
        PRICE.price
    from
        PRICE,
        PART,
        VENDOR,
        LOW_PRICE
    where
        PRICE.price = LOW_PRICE.price AND
        PRICE.part = LOW_PRICE.part AND
        PART.id = PRICE.part AND
        VENDOR.id = PRICE.vendor;
