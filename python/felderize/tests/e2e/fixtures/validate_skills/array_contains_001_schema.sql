-- rule: array_contains
-- spark: array_contains(arr, val) — true if array contains value
-- feldera: ARRAY_CONTAINS(arr, val)
CREATE TABLE product_tags (product_id INT, tag_names ARRAY<STRING>);
