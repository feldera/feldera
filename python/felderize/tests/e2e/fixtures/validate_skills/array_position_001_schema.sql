-- rule: array_position
-- spark: array_position(arr, val) — 1-based index of first occurrence
-- feldera: ARRAY_POSITION(arr, val)
CREATE TABLE products_inv (
  product_id INT,
  name STRING,
  warehouse_codes ARRAY<STRING>
);
