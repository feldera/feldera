-- rule: nvl
-- spark: NVL(a, b) — return b if a is NULL
-- feldera: COALESCE(a, b) — NVL not supported in Feldera
CREATE OR REPLACE TEMP VIEW employee_summary AS SELECT
  emp_id,
  emp_name,
  NVL(department, 'Unassigned') AS dept,
  NVL(salary, CAST(50000.00 AS DECIMAL(10,2))) AS base_salary
FROM employee_info;
