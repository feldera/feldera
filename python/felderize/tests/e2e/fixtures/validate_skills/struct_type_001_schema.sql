-- rule: struct_type
-- spark: STRUCT<a: T, b: U> type in DDL
-- feldera: ROW(a T, b U) — Feldera uses ROW with positional syntax
CREATE TABLE employee_info (
  emp_id INT,
  emp_name STRING,
  details STRUCT<department: STRING, salary: DECIMAL(10,2)>
);
