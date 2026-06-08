-- rule: nullifzero
-- spark: NULLIFZERO(x) — return NULL if x is 0
-- feldera: NULLIF(x, 0)
CREATE TABLE inventory_stock (product_id INT, stock_level INT, reorder_point INT);
