-- rule: cube_agg
-- spark: CUBE(a, b) — all combinations of grouping
-- feldera: CUBE(a, b) — same
CREATE OR REPLACE TEMP VIEW cube_agg_results_002 AS
SELECT
  department,
  shift,
  COUNT(*) as num_records,
  SUM(hours_worked) as total_hours
FROM employee_hours
GROUP BY CUBE(department, shift)
ORDER BY department, shift;
