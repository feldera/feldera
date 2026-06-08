-- rule: upper_lower_length
-- spark: UPPER(s), LOWER(s), LENGTH(s), SUBSTRING(s, pos, len) — basic string functions
-- feldera: UPPER(s) / LOWER(s) / LENGTH(s) / SUBSTRING(s, pos, len) — all work identically in Feldera, no translation needed
CREATE OR REPLACE TEMP VIEW article_view AS SELECT article_id, LENGTH(title) AS title_length, SUBSTRING(title, 1, 5) AS title_start, UPPER(author) AS upper_author, LOWER(body) AS lower_body FROM article_content WHERE LENGTH(title) > 3;
