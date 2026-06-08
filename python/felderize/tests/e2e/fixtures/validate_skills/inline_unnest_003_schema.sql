-- rule: inline_unnest
-- spark: LATERAL VIEW inline(arr_of_structs) t AS f1, f2 — unnest array of structs
-- feldera: UNNEST(arr) AS t(f1, f2) — field names become output columns
CREATE TABLE order_items (order_id INT, order_date DATE, items ARRAY<STRUCT<item_name STRING, quantity INT, price DECIMAL(10,2)>>);
