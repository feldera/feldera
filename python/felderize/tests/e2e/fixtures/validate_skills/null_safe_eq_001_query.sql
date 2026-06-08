-- rule: null_safe_eq
-- spark: a <=> b — null-safe equality (true when both NULL)
-- feldera: a <=> b — same syntax, supported in Feldera
CREATE OR REPLACE TEMP VIEW emp_dept_match AS SELECT e.emp_id, e.name, e.dept, d.dept_name FROM employees e LEFT JOIN departments d ON e.dept <=> d.dept_name;
