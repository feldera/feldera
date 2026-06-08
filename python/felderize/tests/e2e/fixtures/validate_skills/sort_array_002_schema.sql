-- rule: sort_array
-- spark: sort_array(arr) / sort_array(arr, false) — sort array ascending or descending
-- feldera: SORT_ARRAY(arr) / SORT_ARRAY(arr, false)
CREATE TABLE string_scores (item_id INT, category STRING, tags ARRAY<STRING>);
