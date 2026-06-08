-- rule: isnull_isnotnull
-- spark: isnull(x) and isnotnull(x) — null check functions
-- feldera: x IS NULL  /  x IS NOT NULL
CREATE TABLE employees (emp_id INT, emp_name STRING, department STRING, salary DECIMAL(10,2));
