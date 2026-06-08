-- rule: from_unixtime_nofmt
-- spark: from_unixtime(n) — convert Unix epoch seconds (integer) to a TIMESTAMP string
-- feldera: TIMESTAMPADD(SECOND, n, DATE '1970-01-01') — returns TIMESTAMP; Feldera treats as UTC
CREATE TABLE sensor_data (
  sensor_id STRING,
  measurement DOUBLE,
  record_time BIGINT
);
