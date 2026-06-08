-- rule: join_using
-- spark: JOIN ... USING (col) — join on same-named column
-- feldera: JOIN ... USING (col) — same syntax, supported directly in Feldera
CREATE OR REPLACE TEMP VIEW product_inventory_view_v3 AS SELECT p.product_id, p.product_name, i.stock_qty, i.warehouse FROM products_t3 p JOIN inventory_t3 i USING (product_id);
