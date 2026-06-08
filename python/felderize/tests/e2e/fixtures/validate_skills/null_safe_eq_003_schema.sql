-- rule: null_safe_eq
-- spark: a <=> b — null-safe equality (true when both NULL)
-- feldera: a <=> b — same syntax, supported in Feldera
CREATE TABLE products (product_id INT, category STRING, price DECIMAL(10, 2));
CREATE TABLE inventory (inventory_id INT, prod_category STRING, stock INT);
