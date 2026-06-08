-- rule: array_except
-- spark: array_except(a, b) — elements in a not in b
-- feldera: ARRAY_EXCEPT(a, b)
CREATE OR REPLACE TEMP VIEW in_stock_items_v3 AS SELECT menu_id, array_except(available_items, out_of_stock) AS ready_to_serve FROM restaurant_menu_v3;
