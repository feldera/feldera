CREATE TABLE balance_history (account_id BIGINT, snapshot_time TIMESTAMP, balance DECIMAL(12,2)) USING parquet;
