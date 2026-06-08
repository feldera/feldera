CREATE TABLE retail_customers (
  customer_id BIGINT,
  store_id BIGINT,
  region STRING,
  tier STRING
) USING parquet;

CREATE TABLE retail_orders (
  order_id BIGINT,
  customer_id BIGINT,
  store_id BIGINT,
  amount DECIMAL(12,2),
  status STRING,
  order_date DATE
) USING parquet;

CREATE TABLE retail_payments (
  payment_id BIGINT,
  order_id BIGINT,
  payment_amount DECIMAL(12,2),
  payment_method STRING
) USING parquet;

CREATE TABLE retail_stores (
  store_id BIGINT,
  region STRING,
  format STRING
) USING parquet;

CREATE TABLE retail_promotions (
  promo_id BIGINT,
  store_id BIGINT,
  min_amount DECIMAL(12,2),
  max_amount DECIMAL(12,2),
  promo_name STRING
) USING parquet;
