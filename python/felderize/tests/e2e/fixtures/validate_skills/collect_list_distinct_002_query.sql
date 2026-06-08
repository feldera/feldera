-- rule: collect_list_distinct
-- spark: collect_list(distinct col) — aggregate distinct column values per group into an array (deduplicating)
-- feldera: ARRAY_DISTINCT(ARRAY_AGG(col)) — Feldera does not support collect_list(distinct col); use ARRAY_DISTINCT(ARRAY_AGG(col)) instead
CREATE OR REPLACE TEMP VIEW customer_days_v2 AS
SELECT customer_name, collect_list(distinct visit_day) AS visit_days
FROM customer_visits
GROUP BY customer_name
ORDER BY customer_name;
