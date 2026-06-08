CREATE TABLE gateway_a_payments (
  payment_ref STRING,
  order_id BIGINT,
  amount DECIMAL(12,2),
  processed_at TIMESTAMP
) USING parquet;

CREATE TABLE gateway_b_payments (
  payment_ref STRING,
  order_id BIGINT,
  amount DECIMAL(12,2),
  processed_at TIMESTAMP
) USING parquet;
