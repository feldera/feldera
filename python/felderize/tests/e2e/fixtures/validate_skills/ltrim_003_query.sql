-- rule: ltrim
-- spark: LTRIM(s) — removes leading whitespace
-- feldera: TRIM(LEADING FROM s)
CREATE OR REPLACE TEMP VIEW comment_view AS SELECT comment_id, LENGTH(LTRIM(comment_text)) AS trimmed_length, LTRIM(comment_text) AS trimmed_text FROM code_comments;
