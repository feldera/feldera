-- rule: locate
-- spark: LOCATE(substr, str) — find 1-based position of substring
-- feldera: POSITION(substr IN str)
CREATE TABLE product_codes (
  sku STRING,
  product_name STRING,
  batch_id INT
);
