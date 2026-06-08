-- rule: first_value_window
-- spark: FIRST_VALUE(expr) OVER (PARTITION BY ... ORDER BY ...) — first value in window partition
-- feldera: FIRST_VALUE(expr) OVER (PARTITION BY ... ORDER BY ...) — same syntax; ROWS/RANGE frame clauses not supported, partition must be unbounded
CREATE TABLE employee_salary (emp_id INT, dept STRING, salary INT, hire_date DATE);
