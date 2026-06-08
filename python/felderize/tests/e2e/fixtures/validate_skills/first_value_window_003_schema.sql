-- rule: first_value_window
-- spark: FIRST_VALUE(expr) OVER (PARTITION BY ... ORDER BY ...) — first value in window partition
-- feldera: FIRST_VALUE(expr) OVER (PARTITION BY ... ORDER BY ...) — same syntax; ROWS/RANGE frame clauses not supported, partition must be unbounded
CREATE TABLE transaction_log (trans_id INT, customer_id INT, amount DECIMAL(10,2), trans_date TIMESTAMP);
