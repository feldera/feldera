-- rule: nvl2
-- spark: NVL2(a, b, c) — return b if a is NOT NULL, else c
-- feldera: CASE WHEN a IS NOT NULL THEN b ELSE c END
CREATE TABLE product_discount (product_id INT, special_price DECIMAL(10,2), regular_price DECIMAL(10,2), product_name VARCHAR(100));
