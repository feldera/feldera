-- rule: named_struct
-- spark: named_struct('a', v1, 'b', v2) — create a named struct
-- feldera: CAST(ROW(v1, v2) AS ROW(a T, b S)) — use CAST to assign field names
CREATE OR REPLACE TEMP VIEW order_struct_v3 AS SELECT order_id, named_struct('cust', customer_name, 'date', order_date, 'total', order_amount) AS order_info FROM order_details WHERE order_amount >= 100;
