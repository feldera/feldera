-- rule: unix_timestamp
-- spark: unix_timestamp(ts) — seconds since epoch
-- feldera: EXTRACT(EPOCH FROM ts)
CREATE OR REPLACE TEMP VIEW unix_ts_v1 AS SELECT event_id, event_name, event_time, unix_timestamp(event_time) AS epoch_seconds FROM event_log_001;
