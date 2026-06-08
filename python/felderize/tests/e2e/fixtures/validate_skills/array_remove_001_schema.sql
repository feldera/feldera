-- rule: array_remove
-- spark: array_remove(arr, val) — remove all occurrences of val from array
-- feldera: ARRAY_REMOVE(arr, val)
CREATE TABLE inventory_items (item_id INT, tags ARRAY<STRING>);
