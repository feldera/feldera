-- rule: array_union
-- spark: array_union(a, b) — union of two arrays without duplicates
-- feldera: ARRAY_UNION(a, b)
CREATE OR REPLACE TEMP VIEW unified_features_v3 AS SELECT product_id, array_union(feature_set_a, feature_set_b) AS all_features FROM product_variants;
