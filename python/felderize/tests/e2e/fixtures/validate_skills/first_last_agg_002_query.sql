-- rule: first_last_agg
-- spark: first(col) / last(col) — return first or last value in a group
-- feldera: MAX(col) — Feldera has no first()/last() aggregate; use MAX(col) as an approximation
CREATE OR REPLACE TEMP VIEW dept_salary_range AS
SELECT
  dept,
  first(salary) AS initial_salary,
  last(salary) AS final_salary,
  COUNT(*) AS event_count
FROM employee_events
GROUP BY dept;
