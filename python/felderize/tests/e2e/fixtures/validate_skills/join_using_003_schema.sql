-- rule: join_using
-- spark: JOIN ... USING (col) — join on same-named column
-- feldera: JOIN ... USING (col) — same syntax, supported directly in Feldera
CREATE TABLE products_t3 (product_id INT, product_name STRING, category_id INT);
CREATE TABLE inventory_t3 (product_id INT, stock_qty INT, warehouse STRING);
