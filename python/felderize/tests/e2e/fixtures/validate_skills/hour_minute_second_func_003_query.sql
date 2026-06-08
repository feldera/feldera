-- rule: hour_minute_second_func
-- spark: HOUR(ts) — extract hour (0-23); MINUTE(ts) — extract minute (0-59); SECOND(ts) — extract second (0-59)
-- feldera: HOUR(ts) / MINUTE(ts) / SECOND(ts) — all work identically in Feldera, no translation needed
CREATE OR REPLACE TEMP VIEW hourly_readings_v3 AS SELECT sensor_id, reading_time, HOUR(reading_time) AS read_hour, MINUTE(reading_time) AS read_minute, SECOND(reading_time) AS read_second, temperature FROM sensor_data ORDER BY read_hour, read_minute, read_second;
