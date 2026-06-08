-- rule: sec
-- spark: sec(x) — secant (1/cos(x))
-- feldera: SEC(x) — supported directly in Feldera
CREATE OR REPLACE TEMP VIEW sec_results_v2 AS SELECT measurement_id, radian_value, label, sec(radian_value) AS sec_value FROM trigonometric_data WHERE sec(radian_value) IS NOT NULL;
