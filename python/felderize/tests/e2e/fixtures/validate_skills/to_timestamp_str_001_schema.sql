-- rule: to_timestamp_str
-- spark: to_timestamp(str, 'yyyy-MM-dd HH:mm:ss') — parse string to TIMESTAMP; Java format pattern
-- feldera: PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', str) — arg order reversed; translate Java fmt to strftime
CREATE TABLE event_logs (
  log_id INT,
  event_name STRING,
  timestamp_str STRING
);
