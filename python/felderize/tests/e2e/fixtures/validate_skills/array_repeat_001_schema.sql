-- rule: array_repeat
-- spark: array_repeat(val, n) — create array with val repeated n times
-- feldera: ARRAY_REPEAT(val, n)
CREATE TABLE products_001 (product_id INT, product_name STRING, repeat_count INT);
