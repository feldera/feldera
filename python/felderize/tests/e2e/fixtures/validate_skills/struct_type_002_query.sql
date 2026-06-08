-- rule: struct_type
-- spark: STRUCT<a: T, b: U> type in DDL
-- feldera: ROW(a T, b U) — Feldera uses ROW with positional syntax
CREATE OR REPLACE TEMP VIEW product_summary_v2 AS
SELECT
  product_id,
  product_name,
  specs.color,
  specs.weight_kg,
  specs.in_stock
FROM product_catalog
WHERE specs.in_stock = true;
