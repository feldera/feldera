-- rule: from_json_field
-- spark: from_json(json_str, schema) — parse JSON and extract fields
-- feldera: PARSE_JSON(json_str)['field'] + CAST per field
CREATE OR REPLACE TEMP VIEW parsed_events_v1 AS
SELECT
  event_id,
  CAST(from_json(event_json, 'user STRING, action STRING, timestamp LONG').user AS STRING) AS user_name,
  CAST(from_json(event_json, 'user STRING, action STRING, timestamp LONG').action AS STRING) AS action_type,
  CAST(from_json(event_json, 'user STRING, action STRING, timestamp LONG').timestamp AS LONG) AS event_ts
FROM user_events_v1;
