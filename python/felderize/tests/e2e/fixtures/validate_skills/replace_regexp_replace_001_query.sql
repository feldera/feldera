-- rule: replace_regexp_replace
-- spark: REPLACE(s, search, replace) — literal string replacement; REGEXP_REPLACE(s, pattern, replace) — regex-based replacement
-- feldera: REPLACE(s, search, replace) / REGEXP_REPLACE(s, pattern, replace) — both work identically in Feldera, no translation needed
CREATE OR REPLACE TEMP VIEW email_processed AS SELECT id, REPLACE(email, 'oldmail.com', 'newmail.com') AS updated_email, REGEXP_REPLACE(domain, '\\.(com|org)$', '.io') AS new_domain FROM email_logs;
