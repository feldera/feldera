-- rule: array_distinct
-- spark: array_distinct(arr) — remove duplicate elements
-- feldera: ARRAY_DISTINCT(arr)
CREATE OR REPLACE TEMP VIEW product_tags_dedup AS SELECT product_id, array_distinct(tags) AS unique_tags FROM product_tags;
