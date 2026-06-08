-- rule: lag_lead
-- spark: LAG(expr, offset, default) / LEAD(expr, offset, default) — access previous/next row
-- feldera: LAG / LEAD — same syntax
CREATE OR REPLACE TEMP VIEW sales_analysis_v1 AS SELECT id, sale_date, amount, product_name, LAG(amount, 1, 0) OVER (ORDER BY sale_date) AS prev_amount, LEAD(amount, 1, 0) OVER (ORDER BY sale_date) AS next_amount FROM sales_log;
