-- rule: using_parquet
-- spark: CREATE TABLE ... USING parquet (or delta, csv, etc.)
-- feldera: Remove USING clause — Feldera does not use storage format specifiers
CREATE TABLE sales_transactions (transaction_id BIGINT, amount DECIMAL(12,2), transaction_ts TIMESTAMP, region STRING, status STRING);
