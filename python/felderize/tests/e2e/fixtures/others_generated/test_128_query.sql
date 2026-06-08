CREATE OR REPLACE TEMP VIEW bm549_creative_inline AS
SELECT
  order_id,
  sku,
  qty,
  price
FROM creative_line_items
LATERAL VIEW inline(lines) li AS sku, qty, price;
