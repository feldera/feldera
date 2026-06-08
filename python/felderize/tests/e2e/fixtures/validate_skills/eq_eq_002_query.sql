-- rule: eq_eq
-- spark: a == b — double-equals equality operator (Spark allows this)
-- feldera: a = b — use single = in Feldera
CREATE OR REPLACE TEMP VIEW products_filtered_v2 AS SELECT product_id, category, price FROM products_t2 WHERE in_stock == true AND price == CAST(99.99 AS DECIMAL(10,2));
