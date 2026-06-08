-- rule: lag_lead
-- spark: LAG(expr, offset, default) / LEAD(expr, offset, default) — access previous/next row
-- feldera: LAG / LEAD — same syntax
CREATE TABLE sales_log (id INT, sale_date DATE, amount DECIMAL(10, 2), product_name STRING);
