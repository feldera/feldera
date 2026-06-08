-- rule: endswith
-- spark: endswith(s, suffix) — true if string ends with suffix
-- feldera: RIGHT(s, LENGTH(suffix)) = suffix
CREATE TABLE email_log (id INT, email_address STRING, domain STRING);
