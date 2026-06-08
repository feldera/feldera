-- rule: nvl
-- spark: NVL(a, b) — return b if a is NULL
-- feldera: COALESCE(a, b) — NVL not supported in Feldera
CREATE TABLE employee_info (
  emp_id INT,
  emp_name STRING,
  department STRING,
  salary DECIMAL(10,2)
);
