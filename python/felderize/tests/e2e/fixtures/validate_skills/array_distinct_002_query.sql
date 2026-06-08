-- rule: array_distinct
-- spark: array_distinct(arr) — remove duplicate elements
-- feldera: ARRAY_DISTINCT(arr)
CREATE OR REPLACE TEMP VIEW error_codes_unique AS SELECT event_id, array_distinct(codes) AS distinct_codes FROM error_codes;
