-- rule: create_temp_view
-- spark: CREATE OR REPLACE TEMP VIEW / CREATE TEMPORARY VIEW
-- feldera: CREATE VIEW — drop OR REPLACE TEMP / TEMPORARY keywords
CREATE TABLE customer_orders (order_id INT, customer_name STRING, order_amount DECIMAL(10,2), order_date DATE, status STRING);
