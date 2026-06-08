-- rule: array_repeat
-- spark: array_repeat(val, n) — create array with val repeated n times
-- feldera: ARRAY_REPEAT(val, n)
CREATE OR REPLACE TEMP VIEW product_tags_v1 AS SELECT product_id, product_name, array_repeat(product_name, repeat_count) AS tag_array FROM products_001;
