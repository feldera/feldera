-- rule: array_distinct
-- spark: array_distinct(arr) — remove duplicate elements
-- feldera: ARRAY_DISTINCT(arr)
CREATE OR REPLACE TEMP VIEW customer_unique_visits AS SELECT customer_id, array_distinct(visit_dates) AS unique_visit_dates FROM customer_visits;
