-- rule: string_varchar
-- spark: STRING type in CREATE TABLE DDL
-- feldera: VARCHAR — Feldera uses VARCHAR instead of STRING
CREATE OR REPLACE TEMP VIEW product_view AS SELECT product_id, product_name, category FROM products WHERE product_name LIKE '%Pro%';
