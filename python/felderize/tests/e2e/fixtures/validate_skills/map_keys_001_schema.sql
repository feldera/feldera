-- rule: map_keys
-- spark: map_keys(m) — extract keys of a map as array
-- feldera: MAP_KEYS(m)
CREATE TABLE product_attrs (product_id INT, attrs MAP<STRING, STRING>);
