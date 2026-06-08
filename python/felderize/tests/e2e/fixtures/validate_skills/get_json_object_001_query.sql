-- rule: get_json_object
-- spark: get_json_object(json_str, '$.field') — extract a field from JSON string
-- feldera: CAST(PARSE_JSON(json_str)['field'] AS VARCHAR)
CREATE OR REPLACE TEMP VIEW user_profile_extract_v1 AS
SELECT
  user_id,
  get_json_object(profile_json, '$.name') AS extracted_name,
  get_json_object(profile_json, '$.email') AS extracted_email
FROM user_profiles
WHERE user_id > 0;
