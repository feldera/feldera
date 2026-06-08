-- rule: ifnull_func
-- spark: IFNULL(a, b) — return b if a is NULL, otherwise a; equivalent to COALESCE(a, b)
-- feldera: IFNULL(a, b) — same function, supported directly in Feldera; no translation needed
CREATE OR REPLACE TEMP VIEW salary_summary AS SELECT emp_id, IFNULL(base_pay, 50000.0) AS guaranteed_pay, IFNULL(bonus, 0.0) AS total_bonus, IFNULL(base_pay, 50000.0) + IFNULL(bonus, 0.0) AS total_compensation FROM employee_salary;
