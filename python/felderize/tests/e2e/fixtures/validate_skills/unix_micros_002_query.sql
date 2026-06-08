-- rule: unix_micros
-- spark: unix_micros(ts) — microseconds since Unix epoch
-- feldera: CAST(EXTRACT(EPOCH FROM ts) * 1000000 AS BIGINT)
CREATE OR REPLACE TEMP VIEW sensor_micros_v2 AS SELECT sensor_id, unix_micros(reading_ts) AS epoch_micros, value FROM sensor_data_v2 WHERE value > 0;
