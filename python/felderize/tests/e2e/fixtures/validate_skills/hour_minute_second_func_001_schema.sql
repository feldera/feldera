-- rule: hour_minute_second_func
-- spark: HOUR(ts) — extract hour (0-23); MINUTE(ts) — extract minute (0-59); SECOND(ts) — extract second (0-59)
-- feldera: HOUR(ts) / MINUTE(ts) / SECOND(ts) — all work identically in Feldera, no translation needed
CREATE TABLE event_log_v1 (event_id INT, event_time TIMESTAMP, description STRING);
