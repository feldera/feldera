-- rule: ltrim
-- spark: LTRIM(s) — removes leading whitespace
-- feldera: TRIM(LEADING FROM s)
CREATE TABLE code_comments (comment_id INT, comment_text STRING);
