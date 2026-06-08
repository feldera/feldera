-- rule: map_contains_key
-- spark: map_contains_key(m, k) — true if map contains the given key
-- feldera: MAP_CONTAINS_KEY(m, k) — same syntax, supported directly in Feldera
CREATE TABLE product_attrs (product_id INT, attrs MAP<STRING, STRING>);
