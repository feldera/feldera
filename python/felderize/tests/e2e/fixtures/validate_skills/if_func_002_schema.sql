-- rule: if_func
-- spark: IF(condition, true_val, false_val) — conditional expression
-- feldera: CASE WHEN condition THEN true_val ELSE false_val END
CREATE TABLE product_inventory (product_id INT, stock_count INT, price DECIMAL(10,2));
