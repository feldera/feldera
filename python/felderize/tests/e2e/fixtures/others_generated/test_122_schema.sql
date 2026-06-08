CREATE TABLE gap_window_rows (
  grp STRING,
  event_ts TIMESTAMP,
  amount DOUBLE
) USING parquet;
