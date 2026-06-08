-- rule: quarter_func
-- spark: quarter(d) — extract quarter number (1-4) from a DATE or TIMESTAMP
-- feldera: QUARTER(d) — same function name, supported directly in Feldera
CREATE OR REPLACE TEMP VIEW quarterly_sales_v1 AS SELECT transaction_id, sale_date, quarter(sale_date) AS quarter_num, amount FROM sales_q1;
