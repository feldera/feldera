-- rule: zeroifnull
-- spark: ZEROIFNULL(x) — return 0 if x is NULL
-- feldera: COALESCE(x, 0)
CREATE OR REPLACE TEMP VIEW total_compensation AS SELECT
  emp_id,
  base_salary,
  ZEROIFNULL(bonus_amount) AS bonus_safe,
  ZEROIFNULL(commission) AS commission_safe,
  base_salary + ZEROIFNULL(bonus_amount) + ZEROIFNULL(commission) AS total_pay
FROM employee_bonuses;
