-- rule: array_remove
-- spark: array_remove(arr, val) — remove all occurrences of val from array
-- feldera: ARRAY_REMOVE(arr, val)
CREATE TABLE color_palettes (palette_id INT, colors ARRAY<STRING>);
