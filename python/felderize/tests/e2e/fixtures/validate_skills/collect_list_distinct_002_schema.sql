-- rule: collect_list_distinct
-- spark: collect_list(distinct col) — aggregate distinct column values per group into an array (deduplicating)
-- feldera: ARRAY_DISTINCT(ARRAY_AGG(col)) — Feldera does not support collect_list(distinct col); use ARRAY_DISTINCT(ARRAY_AGG(col)) instead
CREATE TABLE customer_visits (visit_id INT, customer_name STRING, visit_day STRING);
CREATE TABLE customer_visits_data AS SELECT * FROM customer_visits WHERE 1=0;
