-- rule: from_json_map_explode
-- spark: explode(from_json(col, 'MAP<STRING,STRING>')) / LATERAL VIEW explode(from_json(col, 'MAP<STRING,STRING>')) — explode a JSON object (parsed as a map) into key/value rows
-- feldera: CROSS JOIN UNNEST(CAST(PARSE_JSON(col) AS MAP<VARCHAR,VARCHAR>)) AS t(key, value) — cast JSON object to MAP then UNNEST to get key/value rows
CREATE OR REPLACE TEMP VIEW sensor_view_003 AS
SELECT
  sensor_id,
  reading_time,
  key AS attr_name,
  value AS attr_val
FROM sensor_data_003
LATERAL VIEW explode(from_json(attributes, 'MAP<STRING,STRING>')) AS key, value
WHERE sensor_id > 1000;
