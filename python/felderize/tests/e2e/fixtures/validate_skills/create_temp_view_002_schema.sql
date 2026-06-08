-- rule: create_temp_view
-- spark: CREATE OR REPLACE TEMP VIEW / CREATE TEMPORARY VIEW
-- feldera: CREATE VIEW — drop OR REPLACE TEMP / TEMPORARY keywords
CREATE TABLE sales_records (sale_id INT, product_name STRING, amount DECIMAL(12,2), sale_timestamp TIMESTAMP);
