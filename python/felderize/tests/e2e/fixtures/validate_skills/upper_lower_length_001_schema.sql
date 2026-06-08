-- rule: upper_lower_length
-- spark: UPPER(s), LOWER(s), LENGTH(s), SUBSTRING(s, pos, len) — basic string functions
-- feldera: UPPER(s) / LOWER(s) / LENGTH(s) / SUBSTRING(s, pos, len) — all work identically in Feldera, no translation needed
CREATE TABLE product_names (id INT, name STRING, description STRING);
