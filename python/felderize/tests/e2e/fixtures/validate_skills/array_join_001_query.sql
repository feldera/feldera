-- rule: array_join
-- spark: array_join(arr, delimiter) — concatenate array elements into string
-- feldera: ARRAY_JOIN(arr, delimiter)
CREATE OR REPLACE TEMP VIEW product_tags_view AS SELECT product_id, array_join(tags, ', ') AS tag_string FROM product_tags;
