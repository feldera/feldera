-- rule: map_values
-- spark: map_values(m) — extract values of a map as array
-- feldera: MAP_VALUES(m)
CREATE TABLE product_ratings (product_id INT, rating_map MAP<STRING, INT>);
