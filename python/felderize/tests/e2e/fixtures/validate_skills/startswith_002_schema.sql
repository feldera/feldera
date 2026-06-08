-- rule: startswith
-- spark: startswith(s, prefix) — true if string starts with prefix
-- feldera: LEFT(s, LENGTH(prefix)) = prefix
CREATE TABLE email_log (user_id INT, email STRING);
