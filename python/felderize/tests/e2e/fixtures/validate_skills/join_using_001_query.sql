-- rule: join_using
-- spark: JOIN ... USING (col) — join on same-named column
-- feldera: JOIN ... USING (col) — same syntax, supported directly in Feldera
CREATE OR REPLACE TEMP VIEW emp_dept_view_v1 AS SELECT e.emp_id, e.emp_name, d.dept_name FROM employees_t1 e JOIN departments_t1 d USING (dept_id);
