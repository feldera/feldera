-- rule: map_keys
-- spark: map_keys(m) — extract keys of a map as array
-- feldera: MAP_KEYS(m)
CREATE OR REPLACE TEMP VIEW product_keys_v1 AS SELECT product_id, map_keys(attrs) AS attribute_keys FROM product_attrs;
