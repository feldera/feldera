-- rule: array_except
-- spark: array_except(a, b) — elements in a not in b
-- feldera: ARRAY_EXCEPT(a, b)
CREATE TABLE product_tags_v1 (product_id INT, all_tags ARRAY<STRING>, excluded_tags ARRAY<STRING>);
