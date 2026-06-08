-- rule: array_distinct
-- spark: array_distinct(arr) — remove duplicate elements
-- feldera: ARRAY_DISTINCT(arr)
CREATE TABLE error_codes (event_id INT, codes ARRAY<INT>);
