-- rule: array_intersect
-- spark: array_intersect(a, b) — intersection of two arrays
-- feldera: ARRAY_INTERSECT(a, b)
CREATE OR REPLACE TEMP VIEW skill_intersection_v1 AS SELECT student_id, array_intersect(primary_skills, secondary_skills) AS common_skills FROM student_skills_v1;
