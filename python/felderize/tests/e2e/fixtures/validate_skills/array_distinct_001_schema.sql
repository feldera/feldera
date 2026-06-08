-- rule: array_distinct
-- spark: array_distinct(arr) — remove duplicate elements
-- feldera: ARRAY_DISTINCT(arr)
CREATE TABLE product_tags (product_id INT, tags ARRAY<STRING>);
