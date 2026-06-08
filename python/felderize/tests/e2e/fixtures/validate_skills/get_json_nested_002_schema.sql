-- rule: get_json_nested
-- spark: get_json_object(json_str, '$.a.b') — extract nested JSON field
-- feldera: CAST(PARSE_JSON(json_str)['a']['b'] AS VARCHAR)
CREATE TABLE order_details (order_id BIGINT, order_json STRING);
