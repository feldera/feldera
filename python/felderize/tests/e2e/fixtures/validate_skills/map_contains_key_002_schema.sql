-- rule: map_contains_key
-- spark: map_contains_key(m, k) — true if map contains the given key
-- feldera: MAP_CONTAINS_KEY(m, k) — same syntax, supported directly in Feldera
CREATE TABLE config_settings (setting_id INT, config_data MAP<STRING, INT>);
