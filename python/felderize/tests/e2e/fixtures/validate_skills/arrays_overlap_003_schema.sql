-- rule: arrays_overlap
-- spark: arrays_overlap(a, b) — true if arrays share any element
-- feldera: ARRAYS_OVERLAP(a, b)
CREATE TABLE user_interests (user_id INT, interests ARRAY<STRING>, hobby_list ARRAY<STRING>);
