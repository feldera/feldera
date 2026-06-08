-- rule: from_json_map_explode
-- spark: explode(from_json(col, 'MAP<STRING,STRING>')) / LATERAL VIEW explode(from_json(col, 'MAP<STRING,STRING>')) — explode a JSON object (parsed as a map) into key/value rows
-- feldera: CROSS JOIN UNNEST(CAST(PARSE_JSON(col) AS MAP<VARCHAR,VARCHAR>)) AS t(key, value) — cast JSON object to MAP then UNNEST to get key/value rows
CREATE OR REPLACE TEMP VIEW events_view_001 AS
SELECT
  event_id,
  event_name,
  key,
  value
FROM events_log_001
LATERAL VIEW explode(from_json(metadata, 'MAP<STRING,STRING>')) AS key, value;
