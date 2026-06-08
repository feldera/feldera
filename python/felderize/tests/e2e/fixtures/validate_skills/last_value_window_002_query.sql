-- rule: last_value_window
-- spark: LAST_VALUE(expr) OVER (PARTITION BY ... ORDER BY ...) — last value in window partition
-- feldera: LAST_VALUE(expr) OVER (PARTITION BY ... ORDER BY ... ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) — must use explicit unbounded ROWS frame; default frame is not supported
CREATE OR REPLACE TEMP VIEW employee_final_bonus_v2 AS SELECT emp_id, dept, bonus_amount, LAST_VALUE(bonus_amount) OVER (PARTITION BY dept ORDER BY bonus_month ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS final_bonus_in_dept FROM employee_bonus_v2;
