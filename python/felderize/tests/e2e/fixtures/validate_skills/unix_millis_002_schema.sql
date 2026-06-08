-- rule: unix_millis
-- spark: unix_millis(ts) — milliseconds since epoch
-- feldera: CAST(EXTRACT(EPOCH FROM ts) * 1000 AS BIGINT)
CREATE TABLE sensor_readings_v2 (sensor_id INT, measurement_time TIMESTAMP, temperature DOUBLE);
