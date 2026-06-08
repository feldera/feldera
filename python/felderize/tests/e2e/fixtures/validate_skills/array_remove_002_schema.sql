-- rule: array_remove
-- spark: array_remove(arr, val) — remove all occurrences of val from array
-- feldera: ARRAY_REMOVE(arr, val)
CREATE TABLE score_records (player_id INT, scores ARRAY<INT>);
