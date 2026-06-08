-- rule: array_exists_hof
-- spark: exists(arr, x -> expr) — true if any element in array satisfies the predicate
-- feldera: ARRAY_EXISTS(arr, x -> expr) — Feldera uses ARRAY_EXISTS instead of exists
CREATE TABLE product_tags (product_id INT, tag_names ARRAY<STRING>);
