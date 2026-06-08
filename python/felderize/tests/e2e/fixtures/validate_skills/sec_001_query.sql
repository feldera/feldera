-- rule: sec
-- spark: sec(x) — secant (1/cos(x))
-- feldera: SEC(x) — supported directly in Feldera
CREATE OR REPLACE TEMP VIEW sec_results_v1 AS SELECT id, angle_radians, sec(angle_radians) AS secant_value FROM angle_measurements;
