-- rule: array_join
-- spark: array_join(arr, delimiter) — concatenate array elements into string
-- feldera: ARRAY_JOIN(arr, delimiter)
CREATE OR REPLACE TEMP VIEW recipe_steps_view AS SELECT recipe_id, array_join(instructions, ' -> ') AS step_sequence FROM recipe_steps;
