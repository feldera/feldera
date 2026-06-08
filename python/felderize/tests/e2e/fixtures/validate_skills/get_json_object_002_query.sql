-- rule: get_json_object
-- spark: get_json_object(json_str, '$.field') — extract a field from JSON string
-- feldera: CAST(PARSE_JSON(json_str)['field'] AS VARCHAR)
CREATE OR REPLACE TEMP VIEW event_detail_extract_v2 AS
SELECT
  event_id,
  get_json_object(event_data, '$.action') AS action_type,
  get_json_object(event_data, '$.timestamp') AS event_time,
  get_json_object(event_data, '$.status') AS status_code
FROM event_logs
WHERE event_id >= 100;
