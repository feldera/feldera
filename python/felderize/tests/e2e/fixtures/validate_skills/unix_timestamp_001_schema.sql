-- rule: unix_timestamp
-- spark: unix_timestamp(ts) — seconds since epoch
-- feldera: EXTRACT(EPOCH FROM ts)
CREATE TABLE event_log_001 (event_id INT, event_name STRING, event_time TIMESTAMP);
