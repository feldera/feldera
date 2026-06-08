-- rule: to_timestamp_str
-- spark: to_timestamp(str, 'yyyy-MM-dd HH:mm:ss') — parse string to TIMESTAMP; Java format pattern
-- feldera: PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', str) — arg order reversed; translate Java fmt to strftime
CREATE OR REPLACE TEMP VIEW sensor_timestamps AS SELECT
  sensor_id,
  measurement,
  to_timestamp(recorded_at, 'yyyy-MM-dd HH:mm:ss') AS ts,
  YEAR(to_timestamp(recorded_at, 'yyyy-MM-dd HH:mm:ss')) AS year,
  MONTH(to_timestamp(recorded_at, 'yyyy-MM-dd HH:mm:ss')) AS month
FROM sensor_data;
