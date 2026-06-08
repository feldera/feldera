-- rule: if_func
-- spark: IF(condition, true_val, false_val) — conditional expression
-- feldera: CASE WHEN condition THEN true_val ELSE false_val END
CREATE OR REPLACE TEMP VIEW inventory_status_v2 AS SELECT product_id, stock_count, IF(stock_count <= 10, 0, price) AS adjusted_price FROM product_inventory;
