-- rule: from_json_field
-- spark: from_json(json_str, schema) — parse JSON and extract fields
-- feldera: PARSE_JSON(json_str)['field'] + CAST per field
CREATE TABLE order_records_v3 (
  order_num INT,
  details_json STRING
);
