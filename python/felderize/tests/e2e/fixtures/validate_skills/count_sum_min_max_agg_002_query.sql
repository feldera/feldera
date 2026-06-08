-- rule: count_sum_min_max_agg
-- spark: COUNT(*) / COUNT(col) / SUM(col) / MIN(col) / MAX(col) — basic aggregate functions
-- feldera: COUNT(*) / COUNT(col) / SUM(col) / MIN(col) / MAX(col) — all work identically in Feldera, no translation needed
CREATE OR REPLACE TEMP VIEW hours_analysis AS SELECT
  department,
  COUNT(*) AS employee_count,
  COUNT(hours_worked) AS recorded_hours,
  SUM(hours_worked) AS total_hours,
  MIN(hours_worked) AS min_hours,
  MAX(hours_worked) AS max_hours
FROM employee_hours
GROUP BY department;
