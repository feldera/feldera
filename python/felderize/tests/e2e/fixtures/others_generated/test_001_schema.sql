CREATE TABLE orders (
  order_id BIGINT,
  customer_id BIGINT,
  region STRING,
  amount DECIMAL(12,2),
  status STRING,
  created_at TIMESTAMP
) USING parquet;
