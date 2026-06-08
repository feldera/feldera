-- rule: map_keys
-- spark: map_keys(m) — extract keys of a map as array
-- feldera: MAP_KEYS(m)
CREATE OR REPLACE TEMP VIEW user_setting_keys_v2 AS SELECT user_id, map_keys(settings) AS setting_names FROM user_preferences WHERE user_id > 0;
