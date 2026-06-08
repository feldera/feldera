-- rule: map_keys
-- spark: map_keys(m) — extract keys of a map as array
-- feldera: MAP_KEYS(m)
CREATE OR REPLACE TEMP VIEW event_info_keys_v3 AS SELECT event_id, map_keys(event_info) AS metadata_keys FROM event_metadata WHERE event_id IS NOT NULL;
