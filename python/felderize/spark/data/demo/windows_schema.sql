CREATE TABLE transactions (
  txn_id BIGINT,
  account_id BIGINT,
  amount DECIMAL(12,2),
  category STRING,
  txn_time TIMESTAMP
) USING parquet;
