-- rule: last_value_window
-- spark: LAST_VALUE(expr) OVER (PARTITION BY ... ORDER BY ...) — last value in window partition
-- feldera: LAST_VALUE(expr) OVER (PARTITION BY ... ORDER BY ... ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) — must use explicit unbounded ROWS frame; default frame is not supported
CREATE OR REPLACE TEMP VIEW sales_summary_v1 AS SELECT id, region, amount, LAST_VALUE(amount) OVER (PARTITION BY region ORDER BY sale_date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_regional_amount FROM sales_log_v1;
