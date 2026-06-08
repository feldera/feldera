-- rule: string_varchar
-- spark: STRING type in CREATE TABLE DDL
-- feldera: VARCHAR — Feldera uses VARCHAR instead of STRING
CREATE TABLE employees (emp_id INT, emp_name STRING, department STRING, email STRING);
