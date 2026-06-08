-- rule: to_timestamp_str
-- spark: to_timestamp(str, 'yyyy-MM-dd HH:mm:ss') — parse string to TIMESTAMP; Java format pattern
-- feldera: PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', str) — arg order reversed; translate Java fmt to strftime
CREATE OR REPLACE TEMP VIEW audit_with_ts AS SELECT
  audit_id,
  action,
  to_timestamp(event_time, 'yyyy-MM-dd HH:mm:ss') AS event_timestamp,
  user_id
FROM audit_trail
ORDER BY to_timestamp(event_time, 'yyyy-MM-dd HH:mm:ss');
