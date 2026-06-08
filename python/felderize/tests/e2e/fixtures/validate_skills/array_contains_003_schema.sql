-- rule: array_contains
-- spark: array_contains(arr, val) — true if array contains value
-- feldera: ARRAY_CONTAINS(arr, val)
CREATE TABLE skill_inventory (employee_id INT, technical_skills ARRAY<STRING>);
