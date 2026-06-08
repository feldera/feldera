CREATE TABLE web_sessions (
  session_id BIGINT,
  customer_id BIGINT,
  region STRING,
  session_start TIMESTAMP,
  duration_seconds INT
) USING parquet;

CREATE TABLE store_visits (
  visit_id BIGINT,
  customer_id BIGINT,
  region STRING,
  visit_start TIMESTAMP,
  duration_minutes INT
) USING parquet;
