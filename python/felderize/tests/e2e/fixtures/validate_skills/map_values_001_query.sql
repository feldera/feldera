-- rule: map_values
-- spark: map_values(m) — extract values of a map as array
-- feldera: MAP_VALUES(m)
CREATE OR REPLACE TEMP VIEW rating_values_v1 AS SELECT product_id, map_values(rating_map) AS values_list FROM product_ratings;
