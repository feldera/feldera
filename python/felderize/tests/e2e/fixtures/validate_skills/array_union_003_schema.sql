-- rule: array_union
-- spark: array_union(a, b) — union of two arrays without duplicates
-- feldera: ARRAY_UNION(a, b)
CREATE TABLE product_variants (product_id INT, feature_set_a ARRAY<INT>, feature_set_b ARRAY<INT>);
