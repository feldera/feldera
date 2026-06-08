-- rule: to_json_func
-- spark: to_json(v) — serialize a struct/map/array value to a JSON string
-- feldera: TO_JSON(v) — same function, supported directly in Feldera
CREATE OR REPLACE TEMP VIEW product_json_v2 AS SELECT product_id, to_json(named_struct('category', category, 'price', price, 'available', in_stock)) AS product_details FROM products_t2 WHERE price > 10.0;
