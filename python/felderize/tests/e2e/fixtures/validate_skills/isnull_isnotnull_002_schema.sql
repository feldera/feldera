-- rule: isnull_isnotnull
-- spark: isnull(x) and isnotnull(x) — null check functions
-- feldera: x IS NULL  /  x IS NOT NULL
CREATE TABLE product_inventory (product_id INT, product_name STRING, category STRING, stock_count INT, last_restock TIMESTAMP);
