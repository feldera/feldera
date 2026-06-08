-- rule: get_json_object
-- spark: get_json_object(json_str, '$.field') — extract a field from JSON string
-- feldera: CAST(PARSE_JSON(json_str)['field'] AS VARCHAR)
CREATE TABLE event_logs (
  event_id BIGINT,
  event_data STRING
);
