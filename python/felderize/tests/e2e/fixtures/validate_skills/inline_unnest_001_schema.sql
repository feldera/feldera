-- rule: inline_unnest
-- spark: LATERAL VIEW inline(arr_of_structs) t AS f1, f2 — unnest array of structs
-- feldera: UNNEST(arr) AS t(f1, f2) — field names become output columns
CREATE TABLE product_reviews (product_id INT, review_scores ARRAY<STRUCT<rating INT, comment STRING>>);
