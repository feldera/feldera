CREATE TABLE session_profiles (
  session_id BIGINT,
  user_id BIGINT,
  tags ARRAY<STRING>,
  attributes MAP<STRING, STRING>,
  event_time TIMESTAMP
) USING parquet;
