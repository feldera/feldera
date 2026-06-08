-- rule: pivot
-- spark: PIVOT(COUNT(col) FOR x IN ('A','B','C')) — pivot rows to columns
-- feldera: NULLIF(COUNT(CASE WHEN x = 'A' THEN col END), 0) AS A, ... — NULLIF wraps COUNT to match Spark NULL semantics (Spark returns NULL for empty buckets, COUNT returns 0)
CREATE OR REPLACE TEMP VIEW event_pivot_v2 AS
SELECT *
FROM event_log
PIVOT (
  COUNT(timestamp) FOR event_type IN ('login', 'logout', 'signup')
);
