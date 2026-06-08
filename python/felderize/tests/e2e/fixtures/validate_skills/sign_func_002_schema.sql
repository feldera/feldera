-- rule: sign_func
-- spark: sign(x) — returns -1.0, 0.0, or 1.0 depending on the sign of x (DOUBLE input/output)
-- feldera: SIGN(x) — same function, supported directly in Feldera
CREATE TABLE temperature_readings (sensor_id INT, location STRING, temp_delta DOUBLE);
