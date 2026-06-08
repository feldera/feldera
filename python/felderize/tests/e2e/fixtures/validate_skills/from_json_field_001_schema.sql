-- rule: from_json_field
-- spark: from_json(json_str, schema) — parse JSON and extract fields
-- feldera: PARSE_JSON(json_str)['field'] + CAST per field
CREATE TABLE user_events_v1 (
  event_id INT,
  event_json STRING
);
