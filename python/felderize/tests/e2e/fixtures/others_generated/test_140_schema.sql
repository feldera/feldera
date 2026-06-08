CREATE TABLE transactions (
  txn_id BIGINT,
  description STRING,
  amount DECIMAL(12, 4)
) USING parquet;
