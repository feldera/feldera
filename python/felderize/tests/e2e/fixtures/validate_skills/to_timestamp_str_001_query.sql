-- rule: to_timestamp_str
-- spark: to_timestamp(str, 'yyyy-MM-dd HH:mm:ss') — parse string to TIMESTAMP; Java format pattern
-- feldera: PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', str) — arg order reversed; translate Java fmt to strftime
CREATE OR REPLACE TEMP VIEW parsed_events AS SELECT
  log_id,
  event_name,
  to_timestamp(timestamp_str, 'yyyy-MM-dd HH:mm:ss') AS parsed_ts
FROM event_logs
WHERE timestamp_str IS NOT NULL;
