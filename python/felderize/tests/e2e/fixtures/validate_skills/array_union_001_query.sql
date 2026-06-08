-- rule: array_union
-- spark: array_union(a, b) — union of two arrays without duplicates
-- feldera: ARRAY_UNION(a, b)
CREATE OR REPLACE TEMP VIEW merged_palette_v1 AS SELECT palette_id, array_union(primary_colors, secondary_colors) AS all_colors FROM color_palette;
