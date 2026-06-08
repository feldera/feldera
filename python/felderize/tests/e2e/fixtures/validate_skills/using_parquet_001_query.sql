-- rule: using_parquet
-- spark: CREATE TABLE ... USING parquet (or delta, csv, etc.)
-- feldera: Remove USING clause — Feldera does not use storage format specifiers
CREATE OR REPLACE TEMP VIEW employee_summary AS SELECT emp_id, emp_name, salary FROM employee_records WHERE salary > 50000;
