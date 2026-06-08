-- rule: string_varchar
-- spark: STRING type in CREATE TABLE DDL
-- feldera: VARCHAR — Feldera uses VARCHAR instead of STRING
CREATE OR REPLACE TEMP VIEW emp_view AS SELECT emp_id, emp_name, department FROM employees WHERE department IS NOT NULL;
