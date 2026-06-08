-- rule: map_contains_key
-- spark: map_contains_key(m, k) — true if map contains the given key
-- feldera: MAP_CONTAINS_KEY(m, k) — same syntax, supported directly in Feldera
CREATE OR REPLACE TEMP VIEW product_attrs_v1 AS SELECT product_id, attrs, map_contains_key(attrs, 'color') AS has_color FROM product_attrs;
