-- rule: array_except
-- spark: array_except(a, b) — elements in a not in b
-- feldera: ARRAY_EXCEPT(a, b)
CREATE TABLE student_scores_v2 (student_id INT, achieved_badges ARRAY<INT>, revoked_badges ARRAY<INT>);
