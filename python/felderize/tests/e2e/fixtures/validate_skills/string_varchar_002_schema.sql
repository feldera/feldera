-- rule: string_varchar
-- spark: STRING type in CREATE TABLE DDL
-- feldera: VARCHAR — Feldera uses VARCHAR instead of STRING
CREATE TABLE products (product_id INT, product_name STRING, category STRING, description STRING, supplier STRING);
