CREATE TABLE order_events (
  order_id BIGINT,
  customer_name STRING,
  status STRING,
  amount DOUBLE
) USING parquet;
