-- rule: map_keys
-- spark: map_keys(m) — extract keys of a map as array
-- feldera: MAP_KEYS(m)
CREATE TABLE event_metadata (event_id INT, event_info MAP<STRING, STRING>);
