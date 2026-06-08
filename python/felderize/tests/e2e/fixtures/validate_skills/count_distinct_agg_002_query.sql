-- rule: count_distinct_agg
-- spark: COUNT(DISTINCT col) — count distinct values in a group
-- feldera: COUNT(DISTINCT col) — works identically in Feldera, no translation needed
CREATE OR REPLACE TEMP VIEW product_diversity_v2 AS SELECT region, COUNT(DISTINCT product_id) AS num_products FROM sales_data GROUP BY region;
