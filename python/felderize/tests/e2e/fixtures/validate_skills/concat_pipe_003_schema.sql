-- rule: concat_pipe
-- spark: s || t — string concatenation using pipe operator (Spark supports this)
-- feldera: s || t — same operator, fully supported in Feldera; pass through as-is
CREATE TABLE products_3 (product_id INT, category STRING, brand STRING, model_code STRING);
