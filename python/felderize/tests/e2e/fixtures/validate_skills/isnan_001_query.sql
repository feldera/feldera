-- rule: isnan
-- spark: isnan(x) — true if x is IEEE 754 NaN
-- feldera: IS_NAN(x)
CREATE OR REPLACE TEMP VIEW sensor_analysis_v1 AS SELECT id, isnan(temperature) AS is_temp_nan, isnan(humidity) AS is_humidity_nan FROM sensor_readings;
