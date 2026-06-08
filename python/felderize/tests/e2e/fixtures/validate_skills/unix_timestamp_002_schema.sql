-- rule: unix_timestamp
-- spark: unix_timestamp(ts) — seconds since epoch
-- feldera: EXTRACT(EPOCH FROM ts)
CREATE TABLE activity_log_002 (activity_id BIGINT, activity_desc STRING, activity_timestamp TIMESTAMP);
