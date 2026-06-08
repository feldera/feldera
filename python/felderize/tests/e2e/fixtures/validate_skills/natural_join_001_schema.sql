-- rule: natural_join
-- spark: NATURAL JOIN — auto-join on all matching column names
-- feldera: NATURAL JOIN — same syntax, supported directly in Feldera
CREATE TABLE employees (emp_id INT, emp_name STRING, dept_id INT);
CREATE TABLE departments (dept_id INT, dept_name STRING, location STRING);
