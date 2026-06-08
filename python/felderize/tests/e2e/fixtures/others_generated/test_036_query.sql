CREATE OR REPLACE TEMP VIEW bm49_email_domains AS
SELECT user_id, SUBSTRING(email, INSTR(email, '@') + 1) AS email_domain FROM email_directory;
