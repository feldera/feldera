-- rule: timestamp_ntz_ddl
-- spark: TIMESTAMP_NTZ type in CREATE TABLE DDL — Spark's timezone-naive timestamp type
-- feldera: TIMESTAMP — Feldera has only one timestamp type; drop the _NTZ suffix
CREATE TABLE sensor_data_t2 (
  sensor_id INT,
  measurement_time TIMESTAMP_NTZ,
  temperature DOUBLE,
  location STRING
);
