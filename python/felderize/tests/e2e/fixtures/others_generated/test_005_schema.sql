CREATE TABLE accounts (
  account_id BIGINT,
  customer_id BIGINT,
  account_status STRING,
  opened_at DATE
) USING parquet;

CREATE TABLE transactions (
  txn_id BIGINT,
  account_id BIGINT,
  txn_type STRING,
  amount DECIMAL(12,2),
  txn_time TIMESTAMP
) USING parquet;

CREATE TABLE disputes (
  dispute_id BIGINT,
  account_id BIGINT,
  status STRING,
  opened_time TIMESTAMP
) USING parquet;
