CREATE TABLE customers (
  customer_id BIGINT,
  customer_name STRING,
  region STRING,
  signup_date DATE
) USING parquet;

CREATE TABLE refunds (
  refund_id BIGINT,
  customer_id BIGINT,
  refund_amount DECIMAL(12,2),
  refund_reason STRING,
  created_at TIMESTAMP
) USING parquet;
