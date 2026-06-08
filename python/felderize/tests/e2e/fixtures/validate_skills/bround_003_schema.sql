-- rule: bround
-- spark: BROUND(x, d) — round using banker's rounding (half-to-even) to d decimal places; use DECIMAL input
-- feldera: BROUND(x, d) — same function, supported for DECIMAL input in Feldera; add note that FLOAT/DOUBLE input is not supported
CREATE TABLE measurement (sensor_id INT, reading DECIMAL(8,3));
