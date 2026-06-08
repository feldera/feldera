-- rule: size
-- spark: size(arr) — number of elements in array; returns -1 for NULL input
-- feldera: COALESCE(CARDINALITY(arr), -1) — CARDINALITY returns NULL for NULL input; COALESCE matches Spark's -1 for NULL
CREATE OR REPLACE TEMP VIEW color_preference_stats_v3 AS SELECT user_id, favorite_colors, size(favorite_colors) AS color_count FROM user_preferences WHERE size(favorite_colors) > 0 OR size(favorite_colors) = -1;
