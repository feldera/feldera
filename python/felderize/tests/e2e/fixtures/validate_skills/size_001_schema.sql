-- rule: size
-- spark: size(arr) — number of elements in array; returns -1 for NULL input
-- feldera: COALESCE(CARDINALITY(arr), -1) — CARDINALITY returns NULL for NULL input; COALESCE matches Spark's -1 for NULL
CREATE TABLE product_tags (product_id INT, tags ARRAY<STRING>);
