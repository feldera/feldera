-- rule: cot
-- spark: cot(x) — cotangent (1/tan(x))
-- feldera: COT(x) — supported directly in Feldera
CREATE OR REPLACE TEMP VIEW cot_results_v1 AS SELECT id, radians, cot(radians) AS cotangent_value FROM angle_measurements;
