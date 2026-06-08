-- rule: unix_millis
-- spark: unix_millis(ts) — milliseconds since epoch
-- feldera: CAST(EXTRACT(EPOCH FROM ts) * 1000 AS BIGINT)
CREATE OR REPLACE TEMP VIEW event_millis_v1 AS SELECT event_id, event_name, unix_millis(event_time) AS time_millis FROM event_log_v1;
