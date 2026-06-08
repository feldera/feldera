-- rule: get_json_nested
-- spark: get_json_object(json_str, '$.a.b') — extract nested JSON field
-- feldera: CAST(PARSE_JSON(json_str)['a']['b'] AS VARCHAR)
CREATE TABLE product_metadata (product_id INT, metadata_json STRING);
