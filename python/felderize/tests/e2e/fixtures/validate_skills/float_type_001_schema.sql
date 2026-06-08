-- rule: float_type
-- spark: FLOAT type in CREATE TABLE DDL
-- feldera: REAL — Feldera uses REAL instead of FLOAT
CREATE TABLE temperature_readings (id INT, sensor_name STRING, celsius_value FLOAT, humidity_percent FLOAT);
