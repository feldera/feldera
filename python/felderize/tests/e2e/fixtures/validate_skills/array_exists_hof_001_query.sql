-- rule: array_exists_hof
-- spark: exists(arr, x -> expr) — true if any element in array satisfies the predicate
-- feldera: ARRAY_EXISTS(arr, x -> expr) — Feldera uses ARRAY_EXISTS instead of exists
CREATE OR REPLACE TEMP VIEW product_tags_view AS SELECT product_id, tag_names FROM product_tags WHERE exists(tag_names, x -> x = 'premium');
