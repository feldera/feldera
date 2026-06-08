CREATE TABLE metric_observations (
  grp STRING,
  subgroup STRING,
  category STRING,
  status STRING,
  user_id BIGINT,
  value DOUBLE,
  amount DECIMAL(12,2),
  quantity INT,
  event_date DATE,
  event_ts TIMESTAMP
) USING parquet;
