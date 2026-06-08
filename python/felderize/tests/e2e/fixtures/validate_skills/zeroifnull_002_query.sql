-- rule: zeroifnull
-- spark: ZEROIFNULL(x) — return 0 if x is NULL
-- feldera: COALESCE(x, 0)
CREATE OR REPLACE TEMP VIEW inventory_available AS SELECT
  product_id,
  ZEROIFNULL(qty_on_hand) AS available_qty,
  ZEROIFNULL(qty_reserved) AS reserved_qty,
  ZEROIFNULL(qty_on_hand) - ZEROIFNULL(qty_reserved) AS net_available
FROM inventory_stock;
