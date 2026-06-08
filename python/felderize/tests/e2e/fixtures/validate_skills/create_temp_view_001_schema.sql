-- rule: create_temp_view
-- spark: CREATE OR REPLACE TEMP VIEW / CREATE TEMPORARY VIEW
-- feldera: CREATE VIEW — drop OR REPLACE TEMP / TEMPORARY keywords
CREATE TABLE employees_t1 (emp_id INT, emp_name STRING, salary DECIMAL(10,2), hire_date DATE);
