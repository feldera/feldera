-- rule: array_union
-- spark: array_union(a, b) — union of two arrays without duplicates
-- feldera: ARRAY_UNION(a, b)
CREATE OR REPLACE TEMP VIEW employee_skills_v2 AS SELECT employee_id, array_union(technical_skills, soft_skills) AS combined_skills FROM skill_inventory;
