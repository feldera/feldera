-- rule: nvl2
-- spark: NVL2(a, b, c) — return b if a is NOT NULL, else c
-- feldera: CASE WHEN a IS NOT NULL THEN b ELSE c END
CREATE OR REPLACE TEMP VIEW bonus_calculation AS SELECT emp_id, NVL2(bonus_pct, bonus_pct, default_bonus) AS effective_bonus FROM employee_info;
