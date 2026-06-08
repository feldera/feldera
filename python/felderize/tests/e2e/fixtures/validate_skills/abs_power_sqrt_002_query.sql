-- rule: abs_power_sqrt
-- spark: ABS(x) — absolute value; POWER(x, p) — exponentiation; SQRT(x) — square root
-- feldera: ABS(x) / POWER(x, p) / SQRT(x) — all work identically in Feldera, no translation needed
CREATE OR REPLACE TEMP VIEW sensor_analysis_v2 AS SELECT sensor_id, ABS(temperature - 20.0) AS temp_deviation, POWER(humidity, 0.5) AS humidity_sqrt, SQRT(ABS(pressure - 1013.25)) AS pressure_variance FROM sensor_data;
