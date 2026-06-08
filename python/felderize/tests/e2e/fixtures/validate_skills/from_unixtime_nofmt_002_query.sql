-- rule: from_unixtime_nofmt
-- spark: from_unixtime(n) — convert Unix epoch seconds (integer) to a TIMESTAMP string
-- feldera: TIMESTAMPADD(SECOND, n, DATE '1970-01-01') — returns TIMESTAMP; Feldera treats as UTC
CREATE OR REPLACE TEMP VIEW sensor_readings_v2 AS SELECT
  sensor_id,
  measurement,
  from_unixtime(record_time) AS timestamp_str
FROM sensor_data
WHERE measurement > 0;
