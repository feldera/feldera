CREATE TABLE page_views (
  view_id BIGINT,
  user_id BIGINT,
  page_url STRING,
  referrer STRING,
  device_type STRING,
  view_duration INT,
  view_time TIMESTAMP
) USING parquet;
