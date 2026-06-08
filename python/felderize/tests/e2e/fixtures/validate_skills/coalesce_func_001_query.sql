-- rule: coalesce_func
-- spark: COALESCE(expr1, expr2, ...) — return first non-NULL value
-- feldera: COALESCE(expr1, expr2, ...) — works identically in Feldera, no translation needed
CREATE OR REPLACE TEMP VIEW employee_names AS SELECT emp_id, COALESCE(middle_name, first_name, 'UNKNOWN') AS display_name FROM employee_info;
