-- rule: named_struct
-- spark: named_struct('a', v1, 'b', v2) — create a named struct
-- feldera: CAST(ROW(v1, v2) AS ROW(a T, b S)) — use CAST to assign field names
CREATE OR REPLACE TEMP VIEW product_struct_v2 AS SELECT product_id, named_struct('name', prod_name, 'category', category, 'quantity', stock_qty) AS prod_info FROM product_data WHERE stock_qty > 0;
