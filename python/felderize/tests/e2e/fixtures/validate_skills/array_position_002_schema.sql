-- rule: array_position
-- spark: array_position(arr, val) — 1-based index of first occurrence
-- feldera: ARRAY_POSITION(arr, val)
CREATE TABLE student_scores (
  student_id INT,
  name STRING,
  test_results ARRAY<INT>
);
