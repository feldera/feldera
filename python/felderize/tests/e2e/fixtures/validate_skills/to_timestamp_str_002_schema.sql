-- rule: to_timestamp_str
-- spark: to_timestamp(str, 'yyyy-MM-dd HH:mm:ss') — parse string to TIMESTAMP; Java format pattern
-- feldera: PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', str) — arg order reversed; translate Java fmt to strftime
CREATE TABLE audit_trail (
  audit_id INT,
  action STRING,
  event_time STRING,
  user_id INT
);
