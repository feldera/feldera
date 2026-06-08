-- rule: create_temp_view
-- spark: CREATE OR REPLACE TEMP VIEW / CREATE TEMPORARY VIEW
-- feldera: CREATE VIEW — drop OR REPLACE TEMP / TEMPORARY keywords
CREATE OR REPLACE TEMP VIEW employee_summary_v1 AS SELECT emp_id, emp_name, salary, hire_date FROM employees_t1 WHERE salary > 50000;
