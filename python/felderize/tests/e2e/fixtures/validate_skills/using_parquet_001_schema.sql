-- rule: using_parquet
-- spark: CREATE TABLE ... USING parquet (or delta, csv, etc.)
-- feldera: Remove USING clause — Feldera does not use storage format specifiers
CREATE TABLE employee_records (emp_id INT, emp_name STRING, salary DECIMAL(10,2), hire_date DATE);
