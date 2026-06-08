CREATE OR REPLACE TEMP VIEW bm42_unique_contacts AS
SELECT email FROM marketing_emails UNION SELECT email FROM support_emails;
