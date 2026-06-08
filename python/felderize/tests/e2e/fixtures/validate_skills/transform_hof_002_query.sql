-- rule: transform_hof
-- spark: transform(arr, x -> expr) — apply lambda to each array element, return transformed array
-- feldera: TRANSFORM(arr, x -> expr) — same syntax, supported directly in Feldera
CREATE OR REPLACE TEMP VIEW normalized_scores_v2 AS SELECT student_id, transform(raw_scores, score -> score + 10) AS adjusted FROM student_scores;
