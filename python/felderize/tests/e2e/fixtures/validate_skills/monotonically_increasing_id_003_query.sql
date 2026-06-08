-- rule: monotonically_increasing_id
-- spark: monotonically_increasing_id() — generates a unique, monotonically increasing 64-bit integer per row using partition metadata; nondeterministic across runs
-- feldera: UNSUPPORTED — Feldera has no equivalent; no stable row identity in streaming mode. Mark unsupported and suggest a surrogate key from existing columns or a sequence.
CREATE OR REPLACE TEMP VIEW product_inventory_v3 AS
SELECT
  monotonically_increasing_id() AS catalog_seq,
  product_id,
  product_name,
  category,
  price,
  stock_qty
FROM product_catalog
WHERE stock_qty > 0
ORDER BY product_id;
