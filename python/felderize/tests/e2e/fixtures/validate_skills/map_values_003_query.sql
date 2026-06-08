-- rule: map_values
-- spark: map_values(m) — extract values of a map as array
-- feldera: MAP_VALUES(m)
CREATE OR REPLACE TEMP VIEW settings_values_v3 AS SELECT config_id, map_values(settings_map) AS setting_values FROM config_settings;
