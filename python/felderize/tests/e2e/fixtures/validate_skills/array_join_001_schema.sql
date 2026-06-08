-- rule: array_join
-- spark: array_join(arr, delimiter) — concatenate array elements into string
-- feldera: ARRAY_JOIN(arr, delimiter)
CREATE TABLE product_tags (product_id INT, tags ARRAY<STRING>);
