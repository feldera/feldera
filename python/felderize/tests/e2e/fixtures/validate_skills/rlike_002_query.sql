-- rule: rlike
-- spark: RLIKE(s, pattern) / s RLIKE pattern — regex match returning Boolean
-- feldera: RLIKE(s, pattern) — same function and infix form, supported directly in Feldera. CRITICAL: Use only simple regex patterns without backslash escapes (e.g. use [.] instead of \\., use [+] instead of \\+, use [0-9] instead of \\d). Feldera SQL string literals do not apply Spark/Java backslash escaping, so \\. in a Spark pattern means literal-backslash-then-any in Feldera.
CREATE OR REPLACE TEMP VIEW phone_pattern_match_v2 AS SELECT id, phone_number, country_code, phone_number RLIKE '^[+]?[0-9]{10,15}$' AS matches_int_format, phone_number RLIKE '^[0-9]{3}[-]?[0-9]{3}[-]?[0-9]{4}$' AS matches_us_format FROM phone_records WHERE phone_number RLIKE '[0-9]';
