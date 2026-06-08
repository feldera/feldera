-- rule: zeroifnull
-- spark: ZEROIFNULL(x) — return 0 if x is NULL
-- feldera: COALESCE(x, 0)
CREATE TABLE inventory_stock (
  product_id INT,
  qty_on_hand INT,
  qty_reserved INT
);
