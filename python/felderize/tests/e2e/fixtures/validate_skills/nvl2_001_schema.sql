-- rule: nvl2
-- spark: NVL2(a, b, c) — return b if a is NOT NULL, else c
-- feldera: CASE WHEN a IS NOT NULL THEN b ELSE c END
CREATE TABLE employee_info (emp_id INT, bonus_pct DECIMAL(5,2), default_bonus DECIMAL(5,2), salary INT);
