-- rule: nvl2
-- spark: NVL2(a, b, c) — return b if a is NOT NULL, else c
-- feldera: CASE WHEN a IS NOT NULL THEN b ELSE c END
CREATE OR REPLACE TEMP VIEW final_price AS SELECT product_id, product_name, NVL2(special_price, special_price, regular_price) AS sale_price FROM product_discount;
