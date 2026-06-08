-- rule: unix_micros
-- spark: unix_micros(ts) — microseconds since Unix epoch
-- feldera: CAST(EXTRACT(EPOCH FROM ts) * 1000000 AS BIGINT)
CREATE TABLE sensor_data_v2 (sensor_id STRING, reading_ts TIMESTAMP, value DOUBLE);
