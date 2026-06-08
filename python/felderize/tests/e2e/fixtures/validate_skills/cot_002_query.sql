-- rule: cot
-- spark: cot(x) — cotangent (1/tan(x))
-- feldera: COT(x) — supported directly in Feldera
CREATE OR REPLACE TEMP VIEW cot_results_v2 AS SELECT measurement_id, angle_value, description, cot(angle_value) AS cot_angle FROM trigonometric_data WHERE cot(angle_value) IS NOT NULL;
