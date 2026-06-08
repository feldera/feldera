-- rule: map_contains_key
-- spark: map_contains_key(m, k) — true if map contains the given key
-- feldera: MAP_CONTAINS_KEY(m, k) — same syntax, supported directly in Feldera
CREATE OR REPLACE TEMP VIEW config_settings_v2 AS SELECT setting_id, config_data, map_contains_key(config_data, 'timeout') AS has_timeout FROM config_settings;
