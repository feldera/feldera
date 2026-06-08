-- rule: date_trunc_day
-- spark: date_trunc('DAY', ts) — truncate timestamp to day
-- feldera: TIMESTAMP_TRUNC(ts, DAY)
CREATE OR REPLACE TEMP VIEW daily_avg_temp_v3 AS SELECT sensor_id, date_trunc('DAY', reading_time) AS reading_day, AVG(temperature) AS avg_temp, MIN(temperature) AS min_temp, MAX(temperature) AS max_temp FROM sensor_readings_3 GROUP BY sensor_id, date_trunc('DAY', reading_time) ORDER BY reading_day, sensor_id;
