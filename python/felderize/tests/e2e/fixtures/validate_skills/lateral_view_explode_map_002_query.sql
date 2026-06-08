-- rule: lateral_view_explode_map
-- spark: LATERAL VIEW explode(map_col) t AS key, val — unnest map into key/value rows
-- feldera: CROSS JOIN UNNEST(map_col) AS t(key, val)
CREATE OR REPLACE TEMP VIEW user_settings_expanded_v2 AS
SELECT up.user_id, up.username, t.key AS setting_name, t.val AS setting_value
FROM user_preferences up
LATERAL VIEW explode(up.settings) t AS key, val;
