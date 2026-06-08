-- rule: rtrim
-- spark: RTRIM(s) — removes trailing whitespace
-- feldera: TRIM(TRAILING FROM s)
CREATE OR REPLACE TEMP VIEW trimmed_products AS SELECT id, RTRIM(name) AS clean_name FROM product_names;
