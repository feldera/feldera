-- rule: array_join
-- spark: array_join(arr, delimiter) — concatenate array elements into string
-- feldera: ARRAY_JOIN(arr, delimiter)
CREATE OR REPLACE TEMP VIEW order_items_view AS SELECT order_id, array_join(item_names, ' | ') AS items_concatenated FROM order_items;
