-- rule: csc
-- spark: csc(x) — cosecant (1/sin(x))
-- feldera: CSC(x) — supported directly in Feldera
CREATE OR REPLACE TEMP VIEW csc_results_v1 AS SELECT id, angle_radians, csc(angle_radians) AS csc_value FROM angle_data;
