-- rule: rtrim
-- spark: RTRIM(s) — removes trailing whitespace
-- feldera: TRIM(TRAILING FROM s)
CREATE TABLE user_feedback (feedback_id INT, comment STRING, rating INT);
