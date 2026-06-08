-- rule: nvl
-- spark: NVL(a, b) — return b if a is NULL
-- feldera: COALESCE(a, b) — NVL not supported in Feldera
CREATE OR REPLACE TEMP VIEW product_view AS SELECT
  product_id,
  product_name,
  NVL(category, 'Miscellaneous') AS product_category,
  NVL(stock_qty, 0) AS available_stock
FROM product_catalog;
