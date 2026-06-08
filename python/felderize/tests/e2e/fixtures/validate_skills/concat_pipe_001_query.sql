-- rule: concat_pipe
-- spark: s || t — string concatenation using pipe operator (Spark supports this)
-- feldera: s || t — same operator, fully supported in Feldera; pass through as-is
CREATE OR REPLACE TEMP VIEW full_names_v1 AS SELECT user_id, first_name || ' ' || last_name AS full_name FROM users_1;
