-- rule: window_count_min_max
-- spark: COUNT(*) OVER (PARTITION BY ... ORDER BY ...) / MIN(col) OVER (...) / MAX(col) OVER (...) — aggregate window functions
-- feldera: COUNT(*) OVER (...) / MIN(col) OVER (...) / MAX(col) OVER (...) — all work identically in Feldera, no translation needed
CREATE TABLE employee_performance (
  emp_id INT,
  department STRING,
  score INT,
  eval_month INT
);
