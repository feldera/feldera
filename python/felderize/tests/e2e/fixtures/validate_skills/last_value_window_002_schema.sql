-- rule: last_value_window
-- spark: LAST_VALUE(expr) OVER (PARTITION BY ... ORDER BY ...) — last value in window partition
-- feldera: LAST_VALUE(expr) OVER (PARTITION BY ... ORDER BY ... ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) — must use explicit unbounded ROWS frame; default frame is not supported
CREATE TABLE employee_bonus_v2 (emp_id INT, dept STRING, bonus_amount DECIMAL(10,2), bonus_month INT);
