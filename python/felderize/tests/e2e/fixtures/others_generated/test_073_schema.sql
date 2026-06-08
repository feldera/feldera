CREATE TABLE text_date_events (
  row_id BIGINT,
  first_name STRING,
  last_name STRING,
  code STRING,
  dirty_text STRING,
  city_name STRING,
  email STRING,
  full_url STRING,
  ts_str STRING,
  dt_str STRING,
  event_ts TIMESTAMP,
  event_date DATE,
  offset_days INT,
  day_number INT,
  metric DOUBLE
) USING parquet;
