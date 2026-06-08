-- rule: isnan
-- spark: isnan(x) — true if x is IEEE 754 NaN
-- feldera: IS_NAN(x)
CREATE TABLE sensor_readings (id INT, temperature DOUBLE, humidity DOUBLE); CREATE TABLE expected_results (id INT, is_temp_nan BOOLEAN, is_humidity_nan BOOLEAN);
