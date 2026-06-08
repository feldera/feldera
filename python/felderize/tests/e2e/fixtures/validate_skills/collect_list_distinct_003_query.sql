-- rule: collect_list_distinct
-- spark: collect_list(distinct col) — aggregate distinct column values per group into an array (deduplicating)
-- feldera: ARRAY_DISTINCT(ARRAY_AGG(col)) — Feldera does not support collect_list(distinct col); use ARRAY_DISTINCT(ARRAY_AGG(col)) instead
CREATE OR REPLACE TEMP VIEW event_tag_list_v3 AS
SELECT event_name, collect_list(distinct tag) AS tags
FROM event_tags
GROUP BY event_name
ORDER BY event_name;
