-- rule: left_anti_join
-- spark: LEFT ANTI JOIN t2 ON cond — rows in t1 with no match in t2
-- feldera: WHERE NOT EXISTS (SELECT 1 FROM t2 WHERE cond)
CREATE TABLE products_catalog (product_id INT, product_name STRING, category STRING);
CREATE TABLE discontinued_items (product_id INT, reason STRING);
