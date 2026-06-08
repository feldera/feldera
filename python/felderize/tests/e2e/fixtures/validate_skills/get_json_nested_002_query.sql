-- rule: get_json_nested
-- spark: get_json_object(json_str, '$.a.b') — extract nested JSON field
-- feldera: CAST(PARSE_JSON(json_str)['a']['b'] AS VARCHAR)
CREATE OR REPLACE TEMP VIEW order_shipping_info AS SELECT order_id, get_json_object(order_json, '$.shipping.address.city') AS city FROM order_details;
