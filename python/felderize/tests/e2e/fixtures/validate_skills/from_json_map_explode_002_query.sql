-- rule: from_json_map_explode
-- spark: explode(from_json(col, 'MAP<STRING,STRING>')) / LATERAL VIEW explode(from_json(col, 'MAP<STRING,STRING>')) — explode a JSON object (parsed as a map) into key/value rows
-- feldera: CROSS JOIN UNNEST(CAST(PARSE_JSON(col) AS MAP<VARCHAR,VARCHAR>)) AS t(key, value) — cast JSON object to MAP then UNNEST to get key/value rows
CREATE OR REPLACE TEMP VIEW config_view_002 AS
SELECT
  config_id,
  config_name,
  key AS prop_key,
  value AS prop_value
FROM config_items_002
LATERAL VIEW explode(from_json(properties, 'MAP<STRING,STRING>')) AS key, value;
