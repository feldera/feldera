-- rule: date_trunc_day
-- spark: date_trunc('DAY', ts) — truncate timestamp to day
-- feldera: TIMESTAMP_TRUNC(ts, DAY)
CREATE OR REPLACE TEMP VIEW event_summary_v1 AS SELECT event_name, date_trunc('DAY', event_timestamp) AS event_day, COUNT(*) AS event_count FROM event_log_1 GROUP BY event_name, date_trunc('DAY', event_timestamp) ORDER BY event_day, event_name;
