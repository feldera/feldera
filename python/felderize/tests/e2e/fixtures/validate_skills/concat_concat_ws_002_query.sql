-- rule: concat_concat_ws
-- spark: CONCAT(s1, s2, ...) — concatenate strings; CONCAT_WS(sep, s1, s2, ...) — concatenate with separator
-- feldera: CONCAT(s1, s2, ...) / CONCAT_WS(sep, s1, s2, ...) — both work identically in Feldera, no translation needed
CREATE OR REPLACE TEMP VIEW product_names_v2 AS
SELECT
  product_id,
  CONCAT(brand, model) AS product_code,
  CONCAT_WS('-', brand, model, color, size) AS full_description
FROM product_details
WHERE product_id <= 10;
