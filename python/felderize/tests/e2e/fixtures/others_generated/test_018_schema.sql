CREATE TABLE regional_orders (
  order_id BIGINT,
  region STRING,
  amount DECIMAL(12,2),
  order_date DATE
) USING parquet;
