-- rule: size
-- spark: size(arr) — number of elements in array; returns -1 for NULL input
-- feldera: COALESCE(CARDINALITY(arr), -1) — CARDINALITY returns NULL for NULL input; COALESCE matches Spark's -1 for NULL
CREATE OR REPLACE TEMP VIEW tag_counts_v1 AS SELECT product_id, size(tags) AS tag_count FROM product_tags;
