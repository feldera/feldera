CREATE TABLE customer_master (
  customer_id BIGINT,
  customer_name STRING,
  signup_date DATE,
  country STRING
) USING parquet;

CREATE TABLE recent_orders (
  order_id BIGINT,
  customer_id BIGINT,
  order_date DATE,
  amount DECIMAL(12,2)
) USING parquet;
