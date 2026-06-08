-- rule: isnull_isnotnull
-- spark: isnull(x) and isnotnull(x) — null check functions
-- feldera: x IS NULL  /  x IS NOT NULL
CREATE OR REPLACE TEMP VIEW inventory_check_v2 AS SELECT product_id, product_name, CASE WHEN isnull(category) THEN 'Uncategorized' ELSE category END as product_category, CASE WHEN isnotnull(stock_count) THEN stock_count ELSE -1 END as available_stock, CASE WHEN isnull(last_restock) THEN 'Never' ELSE 'Restocked' END as restock_status FROM product_inventory;
