-- rule: first_value_window
-- spark: FIRST_VALUE(expr) OVER (PARTITION BY ... ORDER BY ...) — first value in window partition
-- feldera: FIRST_VALUE(expr) OVER (PARTITION BY ... ORDER BY ...) — same syntax; ROWS/RANGE frame clauses not supported, partition must be unbounded
CREATE OR REPLACE TEMP VIEW emp_first_salary_v1 AS SELECT emp_id, dept, salary, FIRST_VALUE(salary) OVER (PARTITION BY dept ORDER BY hire_date) AS first_salary_in_dept FROM employee_salary;
