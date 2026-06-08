-- rule: array_except
-- spark: array_except(a, b) — elements in a not in b
-- feldera: ARRAY_EXCEPT(a, b)
CREATE OR REPLACE TEMP VIEW product_filtered_v1 AS SELECT product_id, array_except(all_tags, excluded_tags) AS remaining_tags FROM product_tags_v1;
