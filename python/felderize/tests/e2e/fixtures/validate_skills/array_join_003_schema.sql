-- rule: array_join
-- spark: array_join(arr, delimiter) — concatenate array elements into string
-- feldera: ARRAY_JOIN(arr, delimiter)
CREATE TABLE recipe_steps (recipe_id INT, instructions ARRAY<STRING>);
