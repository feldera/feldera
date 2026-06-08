-- rule: ifnull_func
-- spark: IFNULL(a, b) — return b if a is NULL, otherwise a; equivalent to COALESCE(a, b)
-- feldera: IFNULL(a, b) — same function, supported directly in Feldera; no translation needed
CREATE OR REPLACE TEMP VIEW inventory_view AS SELECT product_id, IFNULL(stock_level, 0) AS available_stock, IFNULL(warehouse_code, 'UNKNOWN') AS location FROM product_inventory;
