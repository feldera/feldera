-- rule: concat_concat_ws
-- spark: CONCAT(s1, s2, ...) — concatenate strings; CONCAT_WS(sep, s1, s2, ...) — concatenate with separator
-- feldera: CONCAT(s1, s2, ...) / CONCAT_WS(sep, s1, s2, ...) — both work identically in Feldera, no translation needed
CREATE TABLE users_info (
  user_id INT,
  first_name STRING,
  last_name STRING,
  email_domain STRING
);
