-- rule: collect_list_distinct
-- spark: collect_list(distinct col) — aggregate distinct column values per group into an array (deduplicating)
-- feldera: ARRAY_DISTINCT(ARRAY_AGG(col)) — Feldera does not support collect_list(distinct col); use ARRAY_DISTINCT(ARRAY_AGG(col)) instead
CREATE OR REPLACE TEMP VIEW product_distinct_v1 AS
SELECT category, collect_list(distinct product_id) AS product_ids
FROM product_sales
GROUP BY category
ORDER BY category;
