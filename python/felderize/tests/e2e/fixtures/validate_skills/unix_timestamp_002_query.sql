-- rule: unix_timestamp
-- spark: unix_timestamp(ts) — seconds since epoch
-- feldera: EXTRACT(EPOCH FROM ts)
CREATE OR REPLACE TEMP VIEW unix_ts_v2 AS SELECT activity_id, activity_desc, activity_timestamp, unix_timestamp(activity_timestamp) AS seconds_since_epoch FROM activity_log_002;
