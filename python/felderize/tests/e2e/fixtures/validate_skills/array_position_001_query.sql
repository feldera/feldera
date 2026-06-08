-- rule: array_position
-- spark: array_position(arr, val) — 1-based index of first occurrence
-- feldera: ARRAY_POSITION(arr, val)
CREATE OR REPLACE TEMP VIEW product_search_v1 AS
SELECT
  product_id,
  name,
  warehouse_codes,
  array_position(warehouse_codes, 'WH-C') AS position_of_wh_c
FROM products_inv
WHERE array_position(warehouse_codes, 'WH-C') IS NOT NULL;
