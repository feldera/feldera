-- rule: date_trunc_day
-- spark: date_trunc('DAY', ts) — truncate timestamp to day
-- feldera: TIMESTAMP_TRUNC(ts, DAY)
CREATE TABLE event_log_1 (event_id INT, event_timestamp TIMESTAMP, event_name STRING);
