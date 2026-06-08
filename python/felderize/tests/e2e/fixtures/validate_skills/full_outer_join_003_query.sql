-- rule: full_outer_join
-- spark: FULL OUTER JOIN t ON cond — returns all rows from both tables, filling NULLs when there is no match
-- feldera: FULL OUTER JOIN t ON cond — fully supported in Feldera; pass through unchanged
CREATE OR REPLACE TEMP VIEW product_category_v3 AS SELECT p.product_id, p.product_name, p.category_id, c.category_name FROM products_t3 p FULL OUTER JOIN categories_t3 c ON p.category_id = c.category_id ORDER BY p.product_id, c.category_id;
