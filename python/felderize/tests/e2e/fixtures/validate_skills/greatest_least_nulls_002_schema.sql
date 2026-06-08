-- rule: greatest_least_nulls
-- spark: greatest(a, b, ...) / least(a, b, ...) — return maximum/minimum value; Spark skips NULL values
-- feldera: GREATEST_IGNORE_NULLS(a, b, ...) / LEAST_IGNORE_NULLS(a, b, ...) — use the IGNORE_NULLS variant to match Spark's null-skipping semantics
CREATE TABLE score_data (student_id INT, math_score INT, english_score INT, science_score INT);
