-- rule: array_contains
-- spark: array_contains(arr, val) — true if array contains value
-- feldera: ARRAY_CONTAINS(arr, val)
CREATE TABLE user_preferences (user_id INT, favorite_genres ARRAY<STRING>);
