-- rule: from_json_field
-- spark: from_json(json_str, schema) — parse JSON and extract fields
-- feldera: PARSE_JSON(json_str)['field'] + CAST per field
CREATE TABLE product_data_v2 (
  product_id INT,
  metadata_json STRING
);
