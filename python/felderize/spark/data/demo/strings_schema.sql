CREATE TABLE customer_records (
  customer_id BIGINT,
  full_name STRING,
  account_code STRING,
  signup_date STRING,
  notes STRING
) USING parquet;
