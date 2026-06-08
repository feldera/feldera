-- rule: array_contains
-- spark: array_contains(arr, val) — true if array contains value
-- feldera: ARRAY_CONTAINS(arr, val)
CREATE OR REPLACE TEMP VIEW genre_filter_v2 AS SELECT user_id, favorite_genres, array_contains(favorite_genres, 'drama') AS likes_drama FROM user_preferences;
