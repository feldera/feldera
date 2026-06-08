-- rule: first_value_window
-- spark: FIRST_VALUE(expr) OVER (PARTITION BY ... ORDER BY ...) — first value in window partition
-- feldera: FIRST_VALUE(expr) OVER (PARTITION BY ... ORDER BY ...) — same syntax; ROWS/RANGE frame clauses not supported, partition must be unbounded
CREATE OR REPLACE TEMP VIEW cust_first_trans_v3 AS SELECT trans_id, customer_id, amount, FIRST_VALUE(amount) OVER (PARTITION BY customer_id ORDER BY trans_date) AS first_transaction_amount FROM transaction_log;
