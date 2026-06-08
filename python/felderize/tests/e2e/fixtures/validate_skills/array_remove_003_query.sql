-- rule: array_remove
-- spark: array_remove(arr, val) — remove all occurrences of val from array
-- feldera: ARRAY_REMOVE(arr, val)
CREATE OR REPLACE TEMP VIEW filtered_colors_v3 AS SELECT palette_id, array_remove(colors, 'null') AS valid_colors FROM color_palettes;
