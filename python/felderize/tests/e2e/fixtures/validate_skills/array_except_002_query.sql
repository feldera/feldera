-- rule: array_except
-- spark: array_except(a, b) — elements in a not in b
-- feldera: ARRAY_EXCEPT(a, b)
CREATE OR REPLACE TEMP VIEW active_badges_v2 AS SELECT student_id, array_except(achieved_badges, revoked_badges) AS current_badges FROM student_scores_v2;
