-- rule: collect_list_agg
-- spark: collect_list(col) — aggregate all column values per group into an array (preserves duplicates)
-- feldera: ARRAY_AGG(col) — order may differ when source collection is unordered
CREATE OR REPLACE TEMP VIEW review_ratings_v1 AS SELECT product_id, collect_list(rating) as all_ratings FROM product_reviews GROUP BY product_id;
