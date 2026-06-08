-- rule: quarter_func
-- spark: quarter(d) — extract quarter number (1-4) from a DATE or TIMESTAMP
-- feldera: QUARTER(d) — same function name, supported directly in Feldera
CREATE TABLE sales_q1 (transaction_id INT, sale_date DATE, amount DECIMAL(10,2));
