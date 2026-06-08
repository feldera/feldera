-- rule: lateral_view_explode_map
-- spark: LATERAL VIEW explode(map_col) t AS key, val — unnest map into key/value rows
-- feldera: CROSS JOIN UNNEST(map_col) AS t(key, val)
CREATE TABLE inventory_metadata (
  item_id INT,
  item_name STRING,
  quantity INT,
  properties MAP<STRING, STRING>
);

CREATE TABLE property_details (
  item_id INT,
  item_name STRING,
  quantity INT,
  prop_key STRING,
  prop_val STRING
);
