-- rule: get_json_nested
-- spark: get_json_object(json_str, '$.a.b') — extract nested JSON field
-- feldera: CAST(PARSE_JSON(json_str)['a']['b'] AS VARCHAR)
CREATE OR REPLACE TEMP VIEW product_specs AS SELECT product_id, get_json_object(metadata_json, '$.specs.dimensions.height') AS height_value FROM product_metadata;
