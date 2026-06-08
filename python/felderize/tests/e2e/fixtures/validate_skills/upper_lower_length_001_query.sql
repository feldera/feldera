-- rule: upper_lower_length
-- spark: UPPER(s), LOWER(s), LENGTH(s), SUBSTRING(s, pos, len) — basic string functions
-- feldera: UPPER(s) / LOWER(s) / LENGTH(s) / SUBSTRING(s, pos, len) — all work identically in Feldera, no translation needed
CREATE OR REPLACE TEMP VIEW product_view AS SELECT id, UPPER(name) AS upper_name, LOWER(description) AS lower_desc, LENGTH(name) AS name_len FROM product_names WHERE LENGTH(name) > 0;
