CREATE TABLE customer_dim (
  customer_id BIGINT,
  region STRING,
  segment STRING,
  signup_date DATE
) USING parquet;

CREATE TABLE order_fact (
  order_id BIGINT,
  customer_id BIGINT,
  amount DECIMAL(12,2),
  discount DECIMAL(12,2),
  status STRING,
  order_date DATE,
  order_ts TIMESTAMP
) USING parquet;

CREATE TABLE payment_fact (
  order_id BIGINT,
  payment_method STRING,
  payment_amount DECIMAL(12,2)
) USING parquet;
