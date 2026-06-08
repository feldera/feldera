-- rule: left_anti_join
-- spark: LEFT ANTI JOIN t2 ON cond — rows in t1 with no match in t2
-- feldera: WHERE NOT EXISTS (SELECT 1 FROM t2 WHERE cond)
CREATE OR REPLACE TEMP VIEW available_products_v3 AS
SELECT p.product_id, p.product_name, p.category
FROM products_catalog p
LEFT ANTI JOIN discontinued_items d ON p.product_id = d.product_id;
