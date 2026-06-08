-- rule: window_count_min_max
-- spark: COUNT(*) OVER (PARTITION BY ... ORDER BY ...) / MIN(col) OVER (...) / MAX(col) OVER (...) — aggregate window functions
-- feldera: COUNT(*) OVER (...) / MIN(col) OVER (...) / MAX(col) OVER (...) — all work identically in Feldera, no translation needed
CREATE OR REPLACE TEMP VIEW perf_window_v2 AS SELECT
  emp_id,
  department,
  score,
  eval_month,
  COUNT(*) OVER (PARTITION BY department ORDER BY eval_month ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_evals,
  MIN(score) OVER (PARTITION BY department) AS dept_lowest_score,
  MAX(score) OVER (PARTITION BY department ORDER BY eval_month) AS dept_max_score_to_date
FROM employee_performance
ORDER BY department, eval_month;
