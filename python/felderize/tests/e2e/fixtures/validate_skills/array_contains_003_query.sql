-- rule: array_contains
-- spark: array_contains(arr, val) — true if array contains value
-- feldera: ARRAY_CONTAINS(arr, val)
CREATE OR REPLACE TEMP VIEW skill_search_v3 AS SELECT employee_id, technical_skills, array_contains(technical_skills, 'Python') AS knows_python, array_contains(technical_skills, 'Java') AS knows_java FROM skill_inventory;
