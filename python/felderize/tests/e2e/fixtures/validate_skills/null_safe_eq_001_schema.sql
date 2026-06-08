-- rule: null_safe_eq
-- spark: a <=> b — null-safe equality (true when both NULL)
-- feldera: a <=> b — same syntax, supported in Feldera
CREATE TABLE employees (emp_id INT, name STRING, dept STRING);
CREATE TABLE departments (dept_id INT, dept_name STRING);
