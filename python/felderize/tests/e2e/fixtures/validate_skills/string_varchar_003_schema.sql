-- rule: string_varchar
-- spark: STRING type in CREATE TABLE DDL
-- feldera: VARCHAR — Feldera uses VARCHAR instead of STRING
CREATE TABLE orders (order_id INT, customer_name STRING, status STRING, notes STRING);
