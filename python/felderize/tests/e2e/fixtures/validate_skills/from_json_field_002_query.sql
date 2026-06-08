-- rule: from_json_field
-- spark: from_json(json_str, schema) — parse JSON and extract fields
-- feldera: PARSE_JSON(json_str)['field'] + CAST per field
CREATE OR REPLACE TEMP VIEW product_details_v2 AS
SELECT
  product_id,
  CAST(from_json(metadata_json, 'name STRING, price DOUBLE, in_stock BOOLEAN').name AS STRING) AS product_name,
  CAST(from_json(metadata_json, 'name STRING, price DOUBLE, in_stock BOOLEAN').price AS DOUBLE) AS product_price,
  CAST(from_json(metadata_json, 'name STRING, price DOUBLE, in_stock BOOLEAN').in_stock AS BOOLEAN) AS is_available
FROM product_data_v2
WHERE metadata_json IS NOT NULL;
