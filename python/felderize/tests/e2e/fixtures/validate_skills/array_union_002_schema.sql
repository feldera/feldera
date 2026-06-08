-- rule: array_union
-- spark: array_union(a, b) — union of two arrays without duplicates
-- feldera: ARRAY_UNION(a, b)
CREATE TABLE skill_inventory (employee_id INT, technical_skills ARRAY<STRING>, soft_skills ARRAY<STRING>);
