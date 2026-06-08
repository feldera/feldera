-- rule: round_half
-- spark: ROUND(x, d) — Spark rounds half-up (0.5→1); Feldera rounds half-to-even (0.5→0)
-- feldera: ROUND(x, d) — same function; add warning about rounding difference
CREATE TABLE measurement_log (sensor_id INT, temp_celsius DOUBLE, humidity_percent DOUBLE);
