-- rule: coalesce_func
-- spark: COALESCE(expr1, expr2, ...) — return first non-NULL value
-- feldera: COALESCE(expr1, expr2, ...) — works identically in Feldera, no translation needed
CREATE TABLE employee_info (
  emp_id INT,
  first_name STRING,
  middle_name STRING,
  last_name STRING
);
