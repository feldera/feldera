-- rule: sign_func
-- spark: sign(x) — returns -1.0, 0.0, or 1.0 depending on the sign of x (DOUBLE input/output)
-- feldera: SIGN(x) — same function, supported directly in Feldera
CREATE OR REPLACE TEMP VIEW temp_variance_v2 AS SELECT sensor_id, location, temp_delta, sign(temp_delta) AS direction FROM temperature_readings WHERE temp_delta IS NOT NULL;
