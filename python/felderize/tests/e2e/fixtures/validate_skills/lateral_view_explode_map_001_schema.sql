-- rule: lateral_view_explode_map
-- spark: LATERAL VIEW explode(map_col) t AS key, val — unnest map into key/value rows
-- feldera: CROSS JOIN UNNEST(map_col) AS t(key, val)
CREATE TABLE product_attrs (
  product_id INT,
  product_name STRING,
  attributes MAP<STRING, STRING>
);

CREATE TABLE expected_output (
  product_id INT,
  product_name STRING,
  attr_key STRING,
  attr_value STRING
);
