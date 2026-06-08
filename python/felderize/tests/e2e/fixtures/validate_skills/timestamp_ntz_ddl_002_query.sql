-- rule: timestamp_ntz_ddl
-- spark: TIMESTAMP_NTZ type in CREATE TABLE DDL — Spark's timezone-naive timestamp type
-- feldera: TIMESTAMP — Feldera has only one timestamp type; drop the _NTZ suffix
CREATE OR REPLACE TEMP VIEW sensor_view_2 AS SELECT sensor_id, measurement_time, temperature, location FROM sensor_data_t2 WHERE temperature >= 20.0 ORDER BY measurement_time DESC;
