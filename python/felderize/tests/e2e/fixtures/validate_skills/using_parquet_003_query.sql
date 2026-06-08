-- rule: using_parquet
-- spark: CREATE TABLE ... USING parquet (or delta, csv, etc.)
-- feldera: Remove USING clause — Feldera does not use storage format specifiers
CREATE OR REPLACE TEMP VIEW low_stock_items AS SELECT product_id, product_name, quantity_on_hand, warehouse_location FROM product_inventory WHERE quantity_on_hand < 50;
