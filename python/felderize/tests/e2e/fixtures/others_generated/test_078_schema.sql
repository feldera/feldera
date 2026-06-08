CREATE TABLE customer_dim (
  customer_id BIGINT,
  region STRING,
  state STRING,
  country STRING,
  segment STRING,
  signup_date DATE
) USING parquet;

CREATE TABLE order_fact (
  order_id BIGINT,
  customer_id BIGINT,
  region STRING,
  state STRING,
  country STRING,
  amount DECIMAL(12,2),
  order_date DATE,
  order_ts TIMESTAMP,
  status STRING
) USING parquet;

CREATE TABLE payment_fact (
  order_id BIGINT,
  payment_method STRING,
  payment_amount DECIMAL(12,2)
) USING parquet;

CREATE TABLE target_bands (
  region STRING,
  min_amount DECIMAL(12,2),
  max_amount DECIMAL(12,2)
) USING parquet;

CREATE TABLE web_order_fact (
  order_id BIGINT,
  customer_id BIGINT,
  amount DECIMAL(12,2)
) USING parquet;

CREATE TABLE store_order_fact (
  order_id BIGINT,
  customer_id BIGINT,
  amount DECIMAL(12,2)
) USING parquet;

CREATE TABLE partner_order_fact (
  order_id BIGINT,
  customer_id BIGINT,
  amount DECIMAL(12,2)
) USING parquet;
