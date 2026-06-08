-- rule: concat_concat_ws
-- spark: CONCAT(s1, s2, ...) — concatenate strings; CONCAT_WS(sep, s1, s2, ...) — concatenate with separator
-- feldera: CONCAT(s1, s2, ...) / CONCAT_WS(sep, s1, s2, ...) — both work identically in Feldera, no translation needed
CREATE OR REPLACE TEMP VIEW user_emails_v1 AS
SELECT
  user_id,
  CONCAT(first_name, '.', last_name, '@', email_domain) AS full_email,
  CONCAT_WS(' - ', first_name, last_name) AS display_name
FROM users_info
WHERE user_id > 0;
