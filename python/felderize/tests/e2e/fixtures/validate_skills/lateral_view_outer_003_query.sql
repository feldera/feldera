-- rule: lateral_view_outer
-- spark: LATERAL VIEW OUTER explode(arr) t AS item — outer explode: preserves rows where array is NULL or empty (returns NULL for item column)
-- feldera: UNSUPPORTED — Feldera has no OUTER equivalent. UNNEST drops rows where array is NULL or empty. Mark as unsupported and add a warning comment.
CREATE OR REPLACE TEMP VIEW order_sku_breakdown AS
SELECT
  order_id,
  customer_id,
  sku
FROM order_items
LATERAL VIEW OUTER explode(skus) item_list AS sku;
