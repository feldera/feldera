-- rule: rlike
-- spark: RLIKE(s, pattern) / s RLIKE pattern — regex match returning Boolean
-- feldera: RLIKE(s, pattern) — same function and infix form, supported directly in Feldera. CRITICAL: Use only simple regex patterns without backslash escapes (e.g. use [.] instead of \\., use [+] instead of \\+, use [0-9] instead of \\d). Feldera SQL string literals do not apply Spark/Java backslash escaping, so \\. in a Spark pattern means literal-backslash-then-any in Feldera.
CREATE OR REPLACE TEMP VIEW email_validation_v1 AS SELECT id, email_addr, domain, email_addr RLIKE '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+[.][a-zA-Z]{2,}$' AS is_valid_email FROM email_log WHERE email_addr RLIKE '@';
