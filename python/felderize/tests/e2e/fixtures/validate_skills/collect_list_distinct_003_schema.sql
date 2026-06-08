-- rule: collect_list_distinct
-- spark: collect_list(distinct col) — aggregate distinct column values per group into an array (deduplicating)
-- feldera: ARRAY_DISTINCT(ARRAY_AGG(col)) — Feldera does not support collect_list(distinct col); use ARRAY_DISTINCT(ARRAY_AGG(col)) instead
CREATE TABLE event_tags (event_id INT, event_name STRING, tag STRING);
CREATE TABLE event_tags_data AS SELECT * FROM event_tags WHERE 1=0;
