-- rule: join_using
-- spark: JOIN ... USING (col) — join on same-named column
-- feldera: JOIN ... USING (col) — same syntax, supported directly in Feldera
CREATE TABLE employees_t1 (emp_id INT, emp_name STRING, dept_id INT);
CREATE TABLE departments_t1 (dept_id INT, dept_name STRING);
