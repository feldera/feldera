-- rule: using_parquet
-- spark: CREATE TABLE ... USING parquet (or delta, csv, etc.)
-- feldera: Remove USING clause — Feldera does not use storage format specifiers
CREATE TABLE product_inventory (product_id INT, product_name STRING, quantity_on_hand INT, warehouse_location STRING, last_updated DATE);
