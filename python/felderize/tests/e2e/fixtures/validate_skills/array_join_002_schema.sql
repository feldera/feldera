-- rule: array_join
-- spark: array_join(arr, delimiter) — concatenate array elements into string
-- feldera: ARRAY_JOIN(arr, delimiter)
CREATE TABLE order_items (order_id INT, item_names ARRAY<STRING>);
