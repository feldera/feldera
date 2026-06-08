-- rule: concat_pipe
-- spark: s || t — string concatenation using pipe operator (Spark supports this)
-- feldera: s || t — same operator, fully supported in Feldera; pass through as-is
CREATE OR REPLACE TEMP VIEW product_codes_v3 AS SELECT product_id, category || '-' || brand || '-' || model_code AS sku FROM products_3 WHERE category IS NOT NULL;
