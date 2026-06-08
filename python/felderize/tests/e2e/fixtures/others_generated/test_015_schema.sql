CREATE TABLE raw_api_events (
  event_id BIGINT,
  event_time TIMESTAMP,
  payload STRING
) USING parquet;
