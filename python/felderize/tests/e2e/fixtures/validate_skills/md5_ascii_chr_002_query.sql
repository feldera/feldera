-- rule: md5_ascii_chr
-- spark: MD5(s) — MD5 hex digest of string; ASCII(s) — ASCII code of first character; CHR(n) — character from ASCII code
-- feldera: MD5(s) / ASCII(s) / CHR(n) — all work identically in Feldera, no translation needed
CREATE OR REPLACE TEMP VIEW user_auth_v2 AS SELECT user_id, full_name, MD5(email) AS email_hash, ASCII(full_name) AS name_first_ascii, CHR(ASCII(full_name)) AS name_first_char FROM user_names WHERE full_name IS NOT NULL;
