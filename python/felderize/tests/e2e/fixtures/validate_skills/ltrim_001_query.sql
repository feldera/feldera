-- rule: ltrim
-- spark: LTRIM(s) — removes leading whitespace
-- feldera: TRIM(LEADING FROM s)
CREATE OR REPLACE TEMP VIEW product_view AS SELECT id, LTRIM(name) AS trimmed_name FROM product_names;
