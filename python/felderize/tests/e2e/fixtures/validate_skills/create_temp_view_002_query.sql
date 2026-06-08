-- rule: create_temp_view
-- spark: CREATE OR REPLACE TEMP VIEW / CREATE TEMPORARY VIEW
-- feldera: CREATE VIEW — drop OR REPLACE TEMP / TEMPORARY keywords
CREATE OR REPLACE TEMP VIEW high_value_sales_v2 AS SELECT sale_id, product_name, amount, sale_timestamp FROM sales_records WHERE amount >= 1000;
