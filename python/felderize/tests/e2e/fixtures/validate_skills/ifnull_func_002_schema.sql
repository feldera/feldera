-- rule: ifnull_func
-- spark: IFNULL(a, b) — return b if a is NULL, otherwise a; equivalent to COALESCE(a, b)
-- feldera: IFNULL(a, b) — same function, supported directly in Feldera; no translation needed
CREATE TABLE employee_salary (emp_id INT, base_pay DOUBLE, bonus DOUBLE);
