CREATE TABLE payments (
  payment_id BIGINT,
  customer_id BIGINT,
  payment_time TIMESTAMP,
  amount DECIMAL(12,2),
  payment_method STRING
) USING parquet;
