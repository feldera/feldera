-- rule: full_outer_join
-- spark: FULL OUTER JOIN t ON cond — returns all rows from both tables, filling NULLs when there is no match
-- feldera: FULL OUTER JOIN t ON cond — fully supported in Feldera; pass through unchanged
CREATE TABLE employees_t1 (emp_id INT, emp_name STRING, dept_id INT);
CREATE TABLE departments_t1 (dept_id INT, dept_name STRING);
