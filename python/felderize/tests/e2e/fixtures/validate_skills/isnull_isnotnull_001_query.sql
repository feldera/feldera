-- rule: isnull_isnotnull
-- spark: isnull(x) and isnotnull(x) — null check functions
-- feldera: x IS NULL  /  x IS NOT NULL
CREATE OR REPLACE TEMP VIEW null_check_v1 AS SELECT emp_id, emp_name, department, CASE WHEN isnull(department) THEN 'No Dept' ELSE department END as dept_status, CASE WHEN isnotnull(salary) THEN salary ELSE 0 END as effective_salary FROM employees;
