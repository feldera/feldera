-- rule: array_position
-- spark: array_position(arr, val) — 1-based index of first occurrence
-- feldera: ARRAY_POSITION(arr, val)
CREATE OR REPLACE TEMP VIEW order_item_lookup_v3 AS
SELECT
  order_id,
  customer_name,
  items,
  array_position(items, 'Widget-X') AS widget_x_pos,
  CASE
    WHEN array_position(items, 'Widget-X') IS NOT NULL THEN 'Found'
    ELSE 'Not Found'
  END AS widget_status
FROM order_history;
