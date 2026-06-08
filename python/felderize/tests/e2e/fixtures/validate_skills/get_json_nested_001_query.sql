-- rule: get_json_nested
-- spark: get_json_object(json_str, '$.a.b') — extract nested JSON field
-- feldera: CAST(PARSE_JSON(json_str)['a']['b'] AS VARCHAR)
CREATE OR REPLACE TEMP VIEW user_profile_extract AS SELECT id, get_json_object(profile_json, '$.contact.email') AS email_addr FROM user_profiles;
