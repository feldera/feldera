CREATE TABLE brand_orders (customer_id BIGINT, brand_id BIGINT) USING parquet;

CREATE TABLE brands (brand_id BIGINT, brand_name STRING) USING parquet;
