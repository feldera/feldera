-- rule: locate
-- spark: LOCATE(substr, str) — find 1-based position of substring
-- feldera: POSITION(substr IN str)
CREATE OR REPLACE TEMP VIEW code_locations AS SELECT
  sku,
  product_name,
  LOCATE('PRD', sku) AS prd_position,
  LOCATE('-', sku) AS dash_position,
  LOCATE('X', sku) AS x_position
FROM product_codes;
