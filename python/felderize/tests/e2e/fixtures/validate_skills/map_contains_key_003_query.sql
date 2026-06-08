-- rule: map_contains_key
-- spark: map_contains_key(m, k) — true if map contains the given key
-- feldera: MAP_CONTAINS_KEY(m, k) — same syntax, supported directly in Feldera
CREATE OR REPLACE TEMP VIEW user_preferences_v3 AS SELECT user_id, prefs, map_contains_key(prefs, 'notifications_enabled') AS has_notif_pref, map_contains_key(prefs, 'dark_mode') AS has_dark_mode FROM user_preferences;
