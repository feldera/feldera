-- rule: unix_millis
-- spark: unix_millis(ts) — milliseconds since epoch
-- feldera: CAST(EXTRACT(EPOCH FROM ts) * 1000 AS BIGINT)
CREATE OR REPLACE TEMP VIEW sensor_millis_v2 AS SELECT sensor_id, temperature, unix_millis(measurement_time) AS epoch_millis FROM sensor_readings_v2;
