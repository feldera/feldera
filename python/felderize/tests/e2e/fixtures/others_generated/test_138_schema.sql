CREATE TABLE log_entries (
  entry_id BIGINT,
  source STRING,
  raw_path STRING
) USING parquet;
