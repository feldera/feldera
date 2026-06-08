-- rule: get_json_object
-- spark: get_json_object(json_str, '$.field') — extract a field from JSON string
-- feldera: CAST(PARSE_JSON(json_str)['field'] AS VARCHAR)
CREATE OR REPLACE TEMP VIEW product_metadata_extract_v3 AS
SELECT
  product_id,
  get_json_object(metadata_json, '$.sku') AS product_sku,
  get_json_object(metadata_json, '$.price') AS list_price,
  get_json_object(metadata_json, '$.category') AS product_category
FROM product_catalog
WHERE product_id < 1000;
