-- rule: rtrim
-- spark: RTRIM(s) — removes trailing whitespace
-- feldera: TRIM(TRAILING FROM s)
CREATE OR REPLACE TEMP VIEW processed_feedback AS SELECT feedback_id, RTRIM(comment) AS comment_clean, rating FROM user_feedback;
