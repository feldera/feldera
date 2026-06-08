-- rule: pivot
-- spark: PIVOT(COUNT(col) FOR x IN ('A','B','C')) — pivot rows to columns
-- feldera: NULLIF(COUNT(CASE WHEN x = 'A' THEN col END), 0) AS A, ... — NULLIF wraps COUNT to match Spark NULL semantics (Spark returns NULL for empty buckets, COUNT returns 0)
CREATE OR REPLACE TEMP VIEW survey_pivot_v3 AS
SELECT *
FROM survey_responses
PIVOT (
  COUNT(question_id) FOR rating IN ('Poor', 'Good', 'Excellent')
);
