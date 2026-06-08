-- rule: window_sum_avg
-- spark: SUM(col) OVER (PARTITION BY ... ORDER BY ...) / AVG(col) OVER (...) — running aggregate window functions
-- feldera: SUM(col) OVER (...) / AVG(CAST(col AS DOUBLE)) OVER (...) — same window syntax; note AVG on integer input needs CAST to return DOUBLE
CREATE OR REPLACE TEMP VIEW bonus_analysis_v2 AS SELECT emp_id, manager, bonus_amount, SUM(bonus_amount) OVER (PARTITION BY manager ORDER BY bonus_month) AS dept_total_bonus, AVG(bonus_amount) OVER (PARTITION BY manager ORDER BY bonus_month) AS dept_avg_bonus FROM employee_bonus;
