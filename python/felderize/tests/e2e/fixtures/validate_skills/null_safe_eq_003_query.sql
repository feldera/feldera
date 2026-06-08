-- rule: null_safe_eq
-- spark: a <=> b — null-safe equality (true when both NULL)
-- feldera: a <=> b — same syntax, supported in Feldera
CREATE OR REPLACE TEMP VIEW product_inventory AS SELECT p.product_id, p.category, p.price, i.stock FROM products p LEFT JOIN inventory i ON p.category <=> i.prod_category WHERE p.price > CAST(50.00 AS DECIMAL(10,2)) OR p.category <=> i.prod_category;
