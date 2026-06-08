-- rule: array_intersect
-- spark: array_intersect(a, b) — intersection of two arrays
-- feldera: ARRAY_INTERSECT(a, b)
CREATE TABLE student_skills_v1 (student_id INT, primary_skills ARRAY<STRING>, secondary_skills ARRAY<STRING>);
