-- rule: inline_unnest
-- spark: LATERAL VIEW inline(arr_of_structs) t AS f1, f2 — unnest array of structs
-- feldera: UNNEST(arr) AS t(f1, f2) — field names become output columns
CREATE OR REPLACE TEMP VIEW order_line_items_v3 AS SELECT order_id, order_date, item_name, quantity, price FROM order_items LATERAL VIEW inline(items) item_detail AS item_name, quantity, price;
