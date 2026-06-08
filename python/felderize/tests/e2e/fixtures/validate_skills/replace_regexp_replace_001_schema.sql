-- rule: replace_regexp_replace
-- spark: REPLACE(s, search, replace) — literal string replacement; REGEXP_REPLACE(s, pattern, replace) — regex-based replacement
-- feldera: REPLACE(s, search, replace) / REGEXP_REPLACE(s, pattern, replace) — both work identically in Feldera, no translation needed
CREATE TABLE email_logs (id INT, email STRING, domain STRING);
