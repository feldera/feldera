-- rule: using_parquet
-- spark: CREATE TABLE ... USING parquet (or delta, csv, etc.)
-- feldera: Remove USING clause — Feldera does not use storage format specifiers
CREATE OR REPLACE TEMP VIEW high_value_sales AS SELECT transaction_id, amount, region FROM sales_transactions WHERE amount >= 1000 AND status = 'completed';
