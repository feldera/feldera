-- rule: array_exists_hof
-- spark: exists(arr, x -> expr) — true if any element in array satisfies the predicate
-- feldera: ARRAY_EXISTS(arr, x -> expr) — Feldera uses ARRAY_EXISTS instead of exists
CREATE OR REPLACE TEMP VIEW score_records_view AS SELECT record_id, scores FROM score_records WHERE exists(scores, x -> x > 90);
