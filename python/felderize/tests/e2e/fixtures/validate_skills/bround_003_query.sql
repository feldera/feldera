-- rule: bround
-- spark: BROUND(x, d) — round using banker's rounding (half-to-even) to d decimal places; use DECIMAL input
-- feldera: BROUND(x, d) — same function, supported for DECIMAL input in Feldera; add note that FLOAT/DOUBLE input is not supported
CREATE OR REPLACE TEMP VIEW bround_results_v3 AS SELECT sensor_id, reading, BROUND(reading, 0) AS rounded_whole FROM measurement;
