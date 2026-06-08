-- rule: named_struct
-- spark: named_struct('a', v1, 'b', v2) — create a named struct
-- feldera: CAST(ROW(v1, v2) AS ROW(a T, b S)) — use CAST to assign field names
CREATE TABLE product_data (product_id INT, prod_name STRING, category STRING, stock_qty INT);
