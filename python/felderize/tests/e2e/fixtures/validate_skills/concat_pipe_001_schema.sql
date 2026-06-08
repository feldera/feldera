-- rule: concat_pipe
-- spark: s || t — string concatenation using pipe operator (Spark supports this)
-- feldera: s || t — same operator, fully supported in Feldera; pass through as-is
CREATE TABLE users_1 (user_id INT, first_name STRING, last_name STRING);
