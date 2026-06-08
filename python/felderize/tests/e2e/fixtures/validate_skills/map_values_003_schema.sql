-- rule: map_values
-- spark: map_values(m) — extract values of a map as array
-- feldera: MAP_VALUES(m)
CREATE TABLE config_settings (config_id INT, settings_map MAP<STRING, STRING>);
