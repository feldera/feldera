-- rule: inline_unnest
-- spark: LATERAL VIEW inline(arr_of_structs) t AS f1, f2 — unnest array of structs
-- feldera: UNNEST(arr) AS t(f1, f2) — field names become output columns
CREATE OR REPLACE TEMP VIEW review_details_v1 AS SELECT product_id, rating, comment FROM product_reviews LATERAL VIEW inline(review_scores) t AS rating, comment;
