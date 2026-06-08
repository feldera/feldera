-- rule: from_unixtime_nofmt
-- spark: from_unixtime(n) — convert Unix epoch seconds (integer) to a TIMESTAMP string
-- feldera: TIMESTAMPADD(SECOND, n, DATE '1970-01-01') — returns TIMESTAMP; Feldera treats as UTC
CREATE OR REPLACE TEMP VIEW event_timestamps_v1 AS SELECT
  event_id,
  event_name,
  from_unixtime(unix_timestamp) AS event_time
FROM event_logs;
