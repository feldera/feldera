-- rule: transform_hof
-- spark: transform(arr, x -> expr) — apply lambda to each array element, return transformed array
-- feldera: TRANSFORM(arr, x -> expr) — same syntax, supported directly in Feldera
CREATE TABLE student_scores (student_id INT, raw_scores ARRAY<INT>);
