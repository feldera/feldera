-- rule: full_outer_join
-- spark: FULL OUTER JOIN t ON cond — returns all rows from both tables, filling NULLs when there is no match
-- feldera: FULL OUTER JOIN t ON cond — fully supported in Feldera; pass through unchanged
CREATE OR REPLACE TEMP VIEW employee_dept_v1 AS SELECT e.emp_id, e.emp_name, e.dept_id, d.dept_name FROM employees_t1 e FULL OUTER JOIN departments_t1 d ON e.dept_id = d.dept_id;
