-- rule: map_keys
-- spark: map_keys(m) — extract keys of a map as array
-- feldera: MAP_KEYS(m)
CREATE TABLE user_preferences (user_id INT, settings MAP<STRING, INT>);
