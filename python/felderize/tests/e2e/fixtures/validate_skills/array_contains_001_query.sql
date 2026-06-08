-- rule: array_contains
-- spark: array_contains(arr, val) — true if array contains value
-- feldera: ARRAY_CONTAINS(arr, val)
CREATE OR REPLACE TEMP VIEW product_search_v1 AS SELECT product_id, tag_names, array_contains(tag_names, 'electronics') AS is_electronics FROM product_tags;
