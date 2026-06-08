-- rule: struct_type
-- spark: STRUCT<a: T, b: U> type in DDL
-- feldera: ROW(a T, b U) — Feldera uses ROW with positional syntax
CREATE OR REPLACE TEMP VIEW employee_details_v1 AS
SELECT
  emp_id,
  emp_name,
  details.department,
  details.salary
FROM employee_info
WHERE details.salary > 50000;
