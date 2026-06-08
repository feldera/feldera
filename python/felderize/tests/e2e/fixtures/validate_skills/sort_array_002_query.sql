-- rule: sort_array
-- spark: sort_array(arr) / sort_array(arr, false) — sort array ascending or descending
-- feldera: SORT_ARRAY(arr) / SORT_ARRAY(arr, false)
CREATE OR REPLACE TEMP VIEW sorted_tags_v2 AS SELECT item_id, category, sort_array(tags) AS tags_sorted_asc FROM string_scores;
