-- rule: startswith
-- spark: startswith(s, prefix) — true if string starts with prefix
-- feldera: LEFT(s, LENGTH(prefix)) = prefix
CREATE OR REPLACE TEMP VIEW startswith_result_002 AS SELECT user_id, email, startswith(email, 'admin@') AS is_admin_email FROM email_log;
