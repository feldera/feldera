-- rule: startswith
-- spark: startswith(s, prefix) — true if string starts with prefix
-- feldera: LEFT(s, LENGTH(prefix)) = prefix
CREATE OR REPLACE TEMP VIEW startswith_result_001 AS SELECT id, name, startswith(name, 'PROD') AS is_prod_prefix FROM product_names;
