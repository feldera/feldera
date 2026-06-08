-- rule: array_position
-- spark: array_position(arr, val) — 1-based index of first occurrence
-- feldera: ARRAY_POSITION(arr, val)
CREATE TABLE order_history (
  order_id INT,
  customer_name STRING,
  items ARRAY<STRING>
);
