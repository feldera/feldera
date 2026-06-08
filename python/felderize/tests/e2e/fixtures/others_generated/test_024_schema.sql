CREATE TABLE ledger_entries (account_id BIGINT, entry_time TIMESTAMP, entry_type STRING, amount DECIMAL(12,2)) USING parquet;
