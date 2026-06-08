-- rule: timestamp_ntz_ddl
-- spark: TIMESTAMP_NTZ type in CREATE TABLE DDL — Spark's timezone-naive timestamp type
-- feldera: TIMESTAMP — Feldera has only one timestamp type; drop the _NTZ suffix
CREATE TABLE event_log_t1 (
  event_id INT,
  event_name STRING,
  event_time TIMESTAMP_NTZ,
  duration_ms INT
);
