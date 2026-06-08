-- rule: round_half
-- spark: ROUND(x, d) — Spark rounds half-up (0.5→1); Feldera rounds half-to-even (0.5→0)
-- feldera: ROUND(x, d) — same function; add warning about rounding difference
CREATE OR REPLACE TEMP VIEW sensor_readings_v2 AS SELECT sensor_id, ROUND(temp_celsius, 1) AS temp_rounded, ROUND(humidity_percent, 0) AS humidity_rounded FROM measurement_log;
