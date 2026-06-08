-- rule: create_temp_view
-- spark: CREATE OR REPLACE TEMP VIEW / CREATE TEMPORARY VIEW
-- feldera: CREATE VIEW — drop OR REPLACE TEMP / TEMPORARY keywords
CREATE OR REPLACE TEMP VIEW pending_orders_v3 AS SELECT order_id, customer_name, order_amount, order_date, status FROM customer_orders WHERE status = 'pending' OR status = 'processing';
