-- rule: array_union
-- spark: array_union(a, b) — union of two arrays without duplicates
-- feldera: ARRAY_UNION(a, b)
CREATE TABLE color_palette (palette_id INT, primary_colors ARRAY<STRING>, secondary_colors ARRAY<STRING>);
