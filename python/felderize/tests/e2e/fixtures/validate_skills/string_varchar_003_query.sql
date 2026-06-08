-- rule: string_varchar
-- spark: STRING type in CREATE TABLE DDL
-- feldera: VARCHAR — Feldera uses VARCHAR instead of STRING
CREATE OR REPLACE TEMP VIEW order_view AS SELECT order_id, customer_name, status FROM orders WHERE status IN ('Completed', 'Shipped');
