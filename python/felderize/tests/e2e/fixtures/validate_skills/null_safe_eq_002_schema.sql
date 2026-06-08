-- rule: null_safe_eq
-- spark: a <=> b — null-safe equality (true when both NULL)
-- feldera: a <=> b — same syntax, supported in Feldera
CREATE TABLE orders (order_id INT, customer_id INT, status STRING);
CREATE TABLE returns (return_id INT, original_order_id INT, reason STRING);
