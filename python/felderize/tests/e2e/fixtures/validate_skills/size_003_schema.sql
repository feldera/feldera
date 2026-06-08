-- rule: size
-- spark: size(arr) — number of elements in array; returns -1 for NULL input
-- feldera: COALESCE(CARDINALITY(arr), -1) — CARDINALITY returns NULL for NULL input; COALESCE matches Spark's -1 for NULL
CREATE TABLE user_preferences (user_id INT, favorite_colors ARRAY<STRING>);
