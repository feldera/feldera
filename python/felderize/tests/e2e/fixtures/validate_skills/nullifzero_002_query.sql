-- rule: nullifzero
-- spark: NULLIFZERO(x) — return NULL if x is 0
-- feldera: NULLIF(x, 0)
CREATE OR REPLACE TEMP VIEW inventory_nullif_v2 AS SELECT product_id, NULLIFZERO(stock_level) AS available_stock, NULLIFZERO(reorder_point) AS reorder_or_null FROM inventory_stock;
