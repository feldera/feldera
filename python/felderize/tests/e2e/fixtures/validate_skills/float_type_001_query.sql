-- rule: float_type
-- spark: FLOAT type in CREATE TABLE DDL
-- feldera: REAL — Feldera uses REAL instead of FLOAT
CREATE OR REPLACE TEMP VIEW temp_summary_v1 AS SELECT sensor_name, AVG(celsius_value) as avg_temp, MAX(humidity_percent) as max_humidity FROM temperature_readings GROUP BY sensor_name;
