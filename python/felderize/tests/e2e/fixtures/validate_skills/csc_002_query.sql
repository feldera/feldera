-- rule: csc
-- spark: csc(x) — cosecant (1/sin(x))
-- feldera: CSC(x) — supported directly in Feldera
CREATE OR REPLACE TEMP VIEW csc_results_v2 AS SELECT measurement_id, radians, description, csc(radians) AS cosecant FROM trig_measurements WHERE radians > 0.3;
