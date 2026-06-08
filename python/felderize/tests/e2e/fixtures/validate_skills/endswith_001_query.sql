-- rule: endswith
-- spark: endswith(s, suffix) — true if string ends with suffix
-- feldera: RIGHT(s, LENGTH(suffix)) = suffix
CREATE OR REPLACE TEMP VIEW email_domains_v1 AS SELECT id, email_address, domain FROM email_log WHERE endswith(email_address, domain);
