-- rule: array_position
-- spark: array_position(arr, val) — 1-based index of first occurrence
-- feldera: ARRAY_POSITION(arr, val)
CREATE OR REPLACE TEMP VIEW score_analysis_v2 AS
SELECT
  student_id,
  name,
  test_results,
  array_position(test_results, 95) AS first_95_position,
  array_position(test_results, 100) AS first_100_position
FROM student_scores;
