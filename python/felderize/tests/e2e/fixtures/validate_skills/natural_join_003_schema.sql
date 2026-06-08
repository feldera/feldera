-- rule: natural_join
-- spark: NATURAL JOIN — auto-join on all matching column names
-- feldera: NATURAL JOIN — same syntax, supported directly in Feldera
CREATE TABLE products (product_id INT, product_name STRING, category_id INT);
CREATE TABLE categories (category_id INT, category_name STRING, is_active BOOLEAN);
