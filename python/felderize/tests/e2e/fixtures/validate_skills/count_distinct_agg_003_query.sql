-- rule: count_distinct_agg
-- spark: COUNT(DISTINCT col) — count distinct values in a group
-- feldera: COUNT(DISTINCT col) — works identically in Feldera, no translation needed
CREATE OR REPLACE TEMP VIEW customer_categories_v3 AS SELECT customer_id, COUNT(DISTINCT category) AS categories_purchased FROM customer_orders GROUP BY customer_id HAVING COUNT(DISTINCT category) > 1;
