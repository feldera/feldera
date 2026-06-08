-- rule: array_remove
-- spark: array_remove(arr, val) — remove all occurrences of val from array
-- feldera: ARRAY_REMOVE(arr, val)
CREATE OR REPLACE TEMP VIEW scores_without_zeros_v2 AS SELECT player_id, array_remove(scores, 0) AS non_zero_scores FROM score_records;
