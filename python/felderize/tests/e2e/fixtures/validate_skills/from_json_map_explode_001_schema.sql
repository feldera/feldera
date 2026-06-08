-- rule: from_json_map_explode
-- spark: explode(from_json(col, 'MAP<STRING,STRING>')) / LATERAL VIEW explode(from_json(col, 'MAP<STRING,STRING>')) — explode a JSON object (parsed as a map) into key/value rows
-- feldera: CROSS JOIN UNNEST(CAST(PARSE_JSON(col) AS MAP<VARCHAR,VARCHAR>)) AS t(key, value) — cast JSON object to MAP then UNNEST to get key/value rows
CREATE TABLE events_log_001 (
  event_id INT,
  event_name STRING,
  metadata STRING
);
