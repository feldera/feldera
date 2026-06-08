-- rule: arrays_overlap
-- spark: arrays_overlap(a, b) — true if arrays share any element
-- feldera: ARRAYS_OVERLAP(a, b)
CREATE OR REPLACE TEMP VIEW interest_match_v3 AS SELECT user_id, interests, hobby_list, arrays_overlap(interests, hobby_list) AS has_shared_interest FROM user_interests;
