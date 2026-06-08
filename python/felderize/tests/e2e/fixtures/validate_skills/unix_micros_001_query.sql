-- rule: unix_micros
-- spark: unix_micros(ts) — microseconds since Unix epoch
-- feldera: CAST(EXTRACT(EPOCH FROM ts) * 1000000 AS BIGINT)
CREATE OR REPLACE TEMP VIEW event_micros_v1 AS SELECT event_id, unix_micros(event_time) AS micros, description FROM event_log_v1;
