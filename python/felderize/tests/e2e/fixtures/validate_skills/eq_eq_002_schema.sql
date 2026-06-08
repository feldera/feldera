-- rule: eq_eq
-- spark: a == b — double-equals equality operator (Spark allows this)
-- feldera: a = b — use single = in Feldera
CREATE TABLE products_t2 (product_id INT, category STRING, price DECIMAL(10,2), in_stock BOOLEAN);
