-- rule: array_distinct
-- spark: array_distinct(arr) — remove duplicate elements
-- feldera: ARRAY_DISTINCT(arr)
CREATE TABLE customer_visits (customer_id INT, visit_dates ARRAY<STRING>);
