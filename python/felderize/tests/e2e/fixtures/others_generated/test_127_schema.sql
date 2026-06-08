CREATE TABLE creative_ts (
  row_id BIGINT,
  ts TIMESTAMP,
  d DATE,
  tz STRING
) USING parquet;
