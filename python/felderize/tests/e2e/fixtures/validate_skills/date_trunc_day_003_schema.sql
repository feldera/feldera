-- rule: date_trunc_day
-- spark: date_trunc('DAY', ts) — truncate timestamp to day
-- feldera: TIMESTAMP_TRUNC(ts, DAY)
CREATE TABLE sensor_readings_3 (sensor_id INT, reading_time TIMESTAMP, temperature DOUBLE);
