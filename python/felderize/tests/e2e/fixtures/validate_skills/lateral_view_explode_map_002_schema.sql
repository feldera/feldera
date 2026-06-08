-- rule: lateral_view_explode_map
-- spark: LATERAL VIEW explode(map_col) t AS key, val — unnest map into key/value rows
-- feldera: CROSS JOIN UNNEST(map_col) AS t(key, val)
CREATE TABLE user_preferences (
  user_id INT,
  username STRING,
  settings MAP<STRING, STRING>
);

CREATE TABLE settings_expanded (
  user_id INT,
  username STRING,
  setting_name STRING,
  setting_value STRING
);
