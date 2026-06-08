-- rule: if_func
-- spark: IF(condition, true_val, false_val) — conditional expression
-- feldera: CASE WHEN condition THEN true_val ELSE false_val END
CREATE OR REPLACE TEMP VIEW salary_bonus_v1 AS SELECT emp_id, salary, IF(salary > 50000, 'high_earner', 'standard') AS earner_type FROM employee_stats;
