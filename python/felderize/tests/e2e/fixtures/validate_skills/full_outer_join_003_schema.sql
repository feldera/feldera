-- rule: full_outer_join
-- spark: FULL OUTER JOIN t ON cond — returns all rows from both tables, filling NULLs when there is no match
-- feldera: FULL OUTER JOIN t ON cond — fully supported in Feldera; pass through unchanged
CREATE TABLE products_t3 (product_id INT, product_name STRING, category_id INT);
CREATE TABLE categories_t3 (category_id INT, category_name STRING);
