-- rule: unix_millis
-- spark: unix_millis(ts) — milliseconds since epoch
-- feldera: CAST(EXTRACT(EPOCH FROM ts) * 1000 AS BIGINT)
CREATE TABLE event_log_v1 (event_id INT, event_time TIMESTAMP, event_name STRING);
