-- rule: greatest_least_nulls
-- spark: greatest(a, b, ...) / least(a, b, ...) — return maximum/minimum value; Spark skips NULL values
-- feldera: GREATEST_IGNORE_NULLS(a, b, ...) / LEAST_IGNORE_NULLS(a, b, ...) — use the IGNORE_NULLS variant to match Spark's null-skipping semantics
CREATE OR REPLACE TEMP VIEW student_performance_v2 AS SELECT student_id, greatest(math_score, english_score, science_score) AS best_score, least(math_score, english_score, science_score) AS worst_score FROM score_data;
