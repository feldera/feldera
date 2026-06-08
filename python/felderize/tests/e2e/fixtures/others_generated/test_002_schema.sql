CREATE TABLE fact_sales (
  sale_id BIGINT,
  product_id BIGINT,
  store_id BIGINT,
  sold_at TIMESTAMP,
  quantity INT,
  net_amount DECIMAL(12,2)
) USING parquet;

CREATE TABLE dim_product (
  product_id BIGINT,
  category STRING,
  brand STRING,
  unit_price DECIMAL(12,2)
) USING parquet;

CREATE TABLE dim_store (
  store_id BIGINT,
  region STRING,
  format STRING
) USING parquet;
