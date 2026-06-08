-- rule: pmod
-- spark: pmod(a, b) — positive modulo, always non-negative
-- feldera: MOD(MOD(a, b) + b, b)
CREATE OR REPLACE TEMP VIEW pmod_result_v3 AS SELECT sensor_id, reading, threshold, pmod(reading, threshold) AS cyclic_position FROM measurements;
