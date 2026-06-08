-- rule: collect_list_agg
-- spark: collect_list(col) — aggregate all column values per group into an array (preserves duplicates)
-- feldera: ARRAY_AGG(col) — order may differ when source collection is unordered
CREATE TABLE product_reviews (product_id INT, rating INT, reviewer_name STRING);
