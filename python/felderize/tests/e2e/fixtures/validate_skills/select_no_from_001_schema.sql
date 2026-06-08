-- rule: select_no_from
-- spark: SELECT expr GROUP BY ... HAVING ... with no FROM clause
-- feldera: Add dummy FROM: FROM (VALUES (1)) AS t(x)
CREATE TABLE sales_data (product_id INT, amount DECIMAL(10,2), region STRING);
