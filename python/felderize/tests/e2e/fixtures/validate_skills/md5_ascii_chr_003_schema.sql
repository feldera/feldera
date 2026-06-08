-- rule: md5_ascii_chr
-- spark: MD5(s) — MD5 hex digest of string; ASCII(s) — ASCII code of first character; CHR(n) — character from ASCII code
-- feldera: MD5(s) / ASCII(s) / CHR(n) — all work identically in Feldera, no translation needed
CREATE TABLE message_log (msg_id INT, msg_text STRING, sender STRING);
