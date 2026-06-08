-- rule: zeroifnull
-- spark: ZEROIFNULL(x) — return 0 if x is NULL
-- feldera: COALESCE(x, 0)
CREATE TABLE employee_bonuses (
  emp_id INT,
  base_salary DECIMAL(10,2),
  bonus_amount DECIMAL(10,2),
  commission DECIMAL(10,2)
);
