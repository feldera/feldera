-- rule: hour_minute_second_func
-- spark: HOUR(ts) — extract hour (0-23); MINUTE(ts) — extract minute (0-59); SECOND(ts) — extract second (0-59)
-- feldera: HOUR(ts) / MINUTE(ts) / SECOND(ts) — all work identically in Feldera, no translation needed
CREATE OR REPLACE TEMP VIEW time_extract_v1 AS SELECT event_id, event_time, HOUR(event_time) AS hour_val, MINUTE(event_time) AS minute_val, SECOND(event_time) AS second_val, description FROM event_log_v1;
